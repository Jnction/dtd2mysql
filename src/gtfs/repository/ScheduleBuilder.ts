import {Query} from 'mysql2';
import {viaText} from '../../../config/gtfs/vias';
import {RouteType} from "../file/Route";
import {CRS} from '../file/Stop';
import {StopTime} from "../file/StopTime";
import {IdGenerator, STP} from "../native/OverlayRecord";
import {Schedule} from "../native/Schedule";
import {ScheduleCalendar} from "../native/ScheduleCalendar";
import {CIFRepository, ScheduleStopTimeRow} from "./CIFRepository";
import moment = require("moment");

const pickupActivities = ["T ", "TB", "U "];
const dropOffActivities = ["T ", "TF", "D "];
const coordinatedActivity = ["R "];
const notAdvertised = "N ";

/**
 * This class takes a stream of results and builds a list of Schedules
 */
export class ScheduleBuilder {
  private readonly schedules: Schedule[] = [];
  private maxId: number = 0;

  private getTripId(row: ScheduleStopTimeRow) {
    return `${row.train_uid}_${moment(row.runs_from).format('YYYYMMDD')}_${moment(row.runs_to).format('YYYYMMDD')}`;
  }

  /**
   * Take a stream of ScheduleStopTimeRow, turn them into Schedule objects and add the result to the schedules
   */
  public loadSchedules(results: Query): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      let stops: StopTime[] = [];
      let prevRow: ScheduleStopTimeRow | undefined;
      let departureHour = 4;

      results.on("result", (row: ScheduleStopTimeRow) => {
        if (prevRow && prevRow.id !== row.id) {
          this.schedules.push(this.createSchedule(prevRow, stops));
          stops = [];

          departureHour = row.public_arrival_time
            ? parseInt(row.public_arrival_time.substr(0, 2), 10)
            : row.public_departure_time ? parseInt(row.public_departure_time.substr(0, 2), 10) : 4;
        }

        if (row.stp_indicator !== STP.Cancellation) {
          const stop = this.createStop(row, stops.length + 1, departureHour);

          if (prevRow && prevRow.id === row.id && row.crs_code === prevRow.crs_code) {
            if (stop.pickup_type === 0 || stop.drop_off_type === 0) {
              stops[stops.length - 1] = Object.assign(stop, { stop_sequence: stops.length });
            }
          }
          else {
            stops.push(stop);
          }
        }

        prevRow = row;
      });

      results.on("end", () => {
        if (prevRow !== undefined) {
          this.schedules.push(this.createSchedule(prevRow, stops));
        }

        resolve();
      });
      results.on("error", reject);
    });
  }

  private createSchedule(row: ScheduleStopTimeRow, stops: StopTime[]): Schedule {
    this.maxId = Math.max(this.maxId, row.id);

    const mode = routeTypeIndex.hasOwnProperty(row.train_category) ? routeTypeIndex[row.train_category] : RouteType.Rail;

    return new Schedule(
      row.id,
      this.getTripId(row),
      stops,
      row.train_uid,
      row.retail_train_id,
      new ScheduleCalendar(
        moment(row.runs_from),
        moment(row.runs_to),
        {
          0: row.sunday,
          1: row.monday,
          2: row.tuesday,
          3: row.wednesday,
          4: row.thursday,
          5: row.friday,
          6: row.saturday
        }
      ),
      mode,
      row.atoc_code,
      row.stp_indicator,
      mode === RouteType.Rail && row.train_class !== "S",
      row.reservations !== null
    );
  }

  private createStop(row: ScheduleStopTimeRow, stopId: number, departHour: number): StopTime {
    let arrivalTime : string | null = null;
    let departureTime : string | null = null;

    // if either public time is set, use those
    if (row.public_arrival_time || row.public_departure_time) {
      arrivalTime = this.formatTime(row.public_arrival_time, departHour);
      departureTime = this.formatTime(row.public_departure_time, departHour);
    }
    // // if no public time at all (no set down or pick) use the scheduled time
    // else {
    //   arrivalTime = this.formatTime(row.scheduled_arrival_time, departHour);
    //   departureTime = this.formatTime(row.scheduled_departure_time, departHour);
    // }

    const activities = (row.activity ?? '').match(/.{1,2}/g) || [] as string[];
    const pickup = pickupActivities.find(a => activities.includes(a)) && !activities.includes(notAdvertised) ? 0 : 1;
    const coordinatedDropOff = coordinatedActivity.find(a => activities.includes(a)) ? 3 : 0;
    const dropOff = dropOffActivities.find(a => activities.includes(a)) && !activities.includes(notAdvertised) ? 0 : 1;

    return {
      trip_id: this.getTripId(row),
      arrival_time: activities.includes(notAdvertised) ? null : arrivalTime || departureTime,
      departure_time: activities.includes(notAdvertised) ? null : departureTime || arrivalTime,
      stop_id: row.atco_code,
      stop_code: row.crs_code,
      tiploc_code: row.location,
      stop_sequence: stopId,
      stop_headsign: null,
      pickup_type: coordinatedDropOff || pickup,
      drop_off_type: coordinatedDropOff || dropOff,
      shape_dist_traveled: null,
      timepoint: 1
    };
  }

  private formatTime(time: string | null, originDepartureHour: number) {
    if (time === null) return null;

    const departureHour = parseInt(time.substr(0, 2), 10);

    // if the service started after 4am and after the current stops departure hour we've probably rolled over midnight
    if (originDepartureHour >= 4 && originDepartureHour > departureHour) {
      return (departureHour + 24) + time.substr(2);
    }

    return time;
  }

  public get results(): ScheduleResults {
    return {
      schedules: this.schedules,
      idGenerator: this.getIdGenerator(this.maxId)
    };
  }

  private *getIdGenerator(startId: number): IterableIterator<number> {
    let id = startId + 1;
    while (true) {
      yield id++;
    }
  }

  public static async fillStopHeadsigns(schedule : Schedule, repository : CIFRepository) : Promise<void> {
    // use the Darwin timetable reference to generate "via points" 
    const atoc_code = schedule.operator;
    const stops = schedule.stopTimes;
    if (stops.length === 0) return;

    const destination_id = stops[stops.length - 1].stop_id;
    const destination_tiploc = stops[stops.length - 1].tiploc_code;
    const destination_name = await repository.getStopName(destination_id);
    
    for (let i = 0; i < stops.length; ++i) {

      /**
       * Find the index of first call in the stop list after the current stop
       *
       * For multiple calls, specify the start parameter to limit the search after the start such that subsequent calls can be returned
       * @param stop_code
       * @param start
       */
      function findCallingIndex(stop_code : CRS, start = i + 1) : number | null {
        const result = stops.findIndex((stop, index) => stop.stop_code === stop_code && index >= start);
        return result === -1 ? null : result;
      }

      // False destinations still have to be hardcoded currently
      // TODO: guess false destinations from Darwin timetable data instead of hardcoding them
      function getFalseDestination() {
        // https://www.railforums.co.uk/threads/services-advertised-as-terminating-at-penultimate-station.252431/post-6453655
        if (atoc_code === 'SW') {
          const strawberry_hill = findCallingIndex('STW');
          const twickenham = findCallingIndex('TWI');
          const richmond = findCallingIndex('RMD');
          const hounslow = findCallingIndex('HOU');
          const chiswick = findCallingIndex('CHK');
          const kingston = findCallingIndex('KNG');
          const teddington = findCallingIndex('TED');
          const staines = findCallingIndex('SNS');
          const wimbledon = findCallingIndex('WIM');
          const barnes_bridge = findCallingIndex('BNI');
          const brentford = findCallingIndex('BFD');
          const mortlake = findCallingIndex('MTL');
          const barnes = findCallingIndex('BNS');
          const addlestone = findCallingIndex('ASN');

          // Kingston loop clockwise
          if (
              ['WAT', 'VXH', 'CLJ'].includes(stop_code)
              && wimbledon !== null && strawberry_hill !== null && wimbledon < strawberry_hill && staines === null
          ) {
            return 'Strawberry Hill';
          }
          if (kingston !== null && richmond !== null && kingston < richmond) {
            return 'Richmond';
          }
          if (kingston !== null && chiswick !== null && kingston < chiswick) {
            return 'Chiswick';
          }

          // Kingston loop anti-clockwise
          if (
              ['WAT', 'VXH', 'QRB', 'CLJ'].includes(stop_code)
              && teddington !== null && strawberry_hill !== null && twickenham !== null && twickenham < strawberry_hill
          ) {
            return 'Teddington';
          }
          if (wimbledon !== null && (
              twickenham !== null && twickenham < wimbledon || stop_code === 'TWI'
          )) {
            return 'Wimbledon';
          }

          // Hounslow loop clockwise
          if (
              hounslow !== null && richmond !== null && richmond < hounslow 
              && ['WAT', 'VXH', 'QRB', 'CLJ', 'WNT', 'PUT', 'BNS'].includes(stop_code)
          ) {
            return 'Hounslow';
          }
          if (chiswick !== null && (stop_code === 'TWI' || twickenham !== null && twickenham < chiswick)) {
            return 'Chiswick';
          }
          if (stop_code === 'WTN' && barnes_bridge !== null) {
            return 'Barnes Bridge';
          }

          // Hounslow loop anti-clockwise
          if (hounslow !== null && brentford !== null && brentford < hounslow && staines === null) {
            return 'Hounslow';
          }
          if (hounslow !== null && mortlake !== null && hounslow < mortlake) {
            return 'Mortlake';
          }

          // Weybridge (via Hounslow) service
          if (stop_code === 'WAT' && addlestone !== null) {
            return 'Addlestone';
          }
          if (addlestone !== null && barnes !== null && addlestone < barnes) {
            return 'Barnes';
          }
        }

        if (atoc_code === 'SE') {
          const dartford = findCallingIndex('DFD');
          const woolwich = findCallingIndex('WWA');
          const bexleyheath = findCallingIndex('BXH');
          const sidcup = findCallingIndex('SID');
          const slade_green = findCallingIndex('SGR');
          const eltham = findCallingIndex('ELW');
          const crayford = findCallingIndex('CRY');
          const barnehurst = findCallingIndex('BNH');

          // rounder via Woolwich first
          if (woolwich !== null && slade_green !== null && woolwich < slade_green && dartford === null) {
            return 'Slade Green';
          }
          if (slade_green !== null && eltham !== null && slade_green < eltham) {
            return 'Eltham';
          }
          if (slade_green !== null && sidcup !== null && slade_green < sidcup) {
            return 'Sidcup';
          }

          // rounder via Bexleyheath first
          if (bexleyheath !== null && eltham !== null && eltham < bexleyheath && dartford === null) {
            if (slade_green !== null && bexleyheath < slade_green) {
              return `Slade Green`;
            }
            if (barnehurst !== null && bexleyheath < barnehurst) {
              return `Barnehurst`;
            }
          }
          if (barnehurst !== null || stop_code === 'BNH') {
            if (woolwich !== null && (barnehurst ?? i) < woolwich) {
              return 'Woolwich Arsenal';
            }
            if (sidcup !== null && (barnehurst ?? i) < sidcup) {
              return 'Sidcup';
            }
          }

          // rounder via Sidcup first
          if (sidcup !== null && dartford === null) {
            if (slade_green !== null && sidcup < slade_green) {
              return `Slade Green`;
            }
            if (crayford !== null && sidcup < crayford) {
              return `Crayford`;
            }
          }
          if (crayford !== null || stop_code === 'CRY') {
            if (woolwich !== null && (crayford ?? i) < woolwich) {
              return 'Woolwich Arsenal';
            }
            if (eltham !== null && (crayford ?? i) < eltham) {
              return 'Eltham';
            }
          }

          const ashford = findCallingIndex('AFK');
          const sandwich = findCallingIndex('SDW');
          const gravesend = findCallingIndex('GRV');
          const ramsgate = findCallingIndex('RAM');
          const folkestone_west = findCallingIndex('FKW');
          const margate = findCallingIndex('MAR');
          const canterbury_west = findCallingIndex('CBW');
          // Kent coast rounder anti clockwise
          if (
              sandwich !== null 
              && (ashford !== null && ashford < sandwich || stop_code === 'AFK') 
              && canterbury_west === null /* https://github.com/planarnetwork/dtd2mysql/issues/80 */) {
            return 'Sandwich';
          }
          if (gravesend !== null && (stop_code === 'SDW' || (sandwich !== null && sandwich < gravesend))) {
            return 'Gravesend';
          }

          // Kent coast rounder clockwise
          if (ramsgate !== null && gravesend !== null && gravesend < ramsgate) {
            return 'Ramsgate';
          }
          if (folkestone_west !== null && margate !== null && margate < folkestone_west) {
            return 'Folkestone West';
          }
        }
        
        if (atoc_code === 'ME') {
          // Wirral line
          const moorfields = findCallingIndex('MRF');
          const liverpool_central = findCallingIndex('LVC');
          if (moorfields !== null && liverpool_central !== null && moorfields < liverpool_central) {
            return 'Liverpool Central';
          }
        }
        
        if (atoc_code === 'AW') {
          // Merthyr line
          const cardiff_central = findCallingIndex('CDF');
          if (cardiff_central !== null) {
            const radyr = findCallingIndex('RDR', cardiff_central);
            if (radyr !== null) {
              return 'Cardiff Central';
            }
          }
        }

        {
          const huddersfield = findCallingIndex('HUD', i);
          if (huddersfield !== null) {
            const brighouse = findCallingIndex('BGH', huddersfield);
            if (brighouse !== null) {
              if (findCallingIndex('HUD', brighouse) /* again */) {
                return 'Brighouse';
              }
            }
          }
        }
        return null;
      }

      const stop = stops[i];
      const stop_code = stop.stop_code ?? '';
      const false_destination = getFalseDestination();

      const remaining_tiplocs = stops.slice(i + 1).map(s => s.tiploc_code);

      const via = viaText[stop_code]?.find(
          item => {
            const loc1index = item.Loc1 === null ? null : remaining_tiplocs.indexOf(item.Loc1);
            const loc2index = item.Loc2 === null ? null : remaining_tiplocs.indexOf(item.Loc2);
            return item.At === stop_code && item.Dest === destination_tiploc
              && (item.Loc1 === null || loc1index! >= 0) && (item.Loc2 === null || loc2index! >= 0)
              && (item.Loc1 === null || item.Loc2 === null || loc2index! > loc1index!)
          }
      )?.Viatext;
      
      stop.stop_headsign = via !== undefined ? `${false_destination ?? destination_name} (${via})` : false_destination;
    }
  }
}

export interface ScheduleResults {
  schedules: Schedule[],
  idGenerator: IdGenerator
}

const routeTypeIndex: object = {
  "OO": RouteType.Rail,
  "XX": RouteType.Rail,
  "XZ": RouteType.Rail,
  "BR": RouteType.ReplacementBus,
  "BS": RouteType.Bus,
  "OL": RouteType.Subway,
  "XC": RouteType.Rail,
  "SS": RouteType.Ferry
};
