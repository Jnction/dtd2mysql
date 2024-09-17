import {Query} from 'mysql2';
import {viaText} from '../../../config/gtfs/vias';
import {RouteType} from "../file/Route";
import {CRS} from '../file/Stop';
import {StopTime} from "../file/StopTime";
import {IdGenerator, STP} from "../native/OverlayRecord";
import {Schedule} from "../native/Schedule";
import {ScheduleCalendar} from "../native/ScheduleCalendar";
import {CIFRepository, ScheduleStopTimeRow, ViaText} from "./CIFRepository";
import moment = require("moment");
import { agencies } from "../../../config/gtfs/agency";

const pickupActivities = ["T ", "TB", "TF", "U "];
const dropOffActivities = ["T ", "TB", "TF", "D "];
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
      agencies.some((a) => a.agency_id === `=${row.atoc_code}`) ? row.atoc_code : "ZZ",
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
      /**
       * Find the index of the false destination
       */
      function getFalseDestinationIndex() {
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
            return strawberry_hill;
          }
          if (kingston !== null && richmond !== null && kingston < richmond) {
            return richmond;
          }
          if (kingston !== null && chiswick !== null && kingston < chiswick) {
            return chiswick;
          }

          // Kingston loop anti-clockwise
          if (
              ['WAT', 'VXH', 'QRB', 'CLJ'].includes(stop_code)
              && teddington !== null && strawberry_hill !== null && twickenham !== null && twickenham < strawberry_hill
          ) {
            return teddington;
          }
          if (wimbledon !== null && (
              twickenham !== null && twickenham < wimbledon || stop_code === 'TWI'
          )) {
            return wimbledon;
          }

          // Hounslow loop clockwise
          if (
              hounslow !== null && richmond !== null && richmond < hounslow 
              && ['WAT', 'VXH', 'QRB', 'CLJ', 'WNT', 'PUT', 'BNS'].includes(stop_code)
          ) {
            return hounslow;
          }
          if (chiswick !== null && (stop_code === 'TWI' || twickenham !== null && twickenham < chiswick)) {
            return chiswick;
          }
          if (stop_code === 'WTN' && barnes_bridge !== null) {
            return barnes_bridge;
          }

          // Hounslow loop anti-clockwise
          if (hounslow !== null && brentford !== null && brentford < hounslow && staines === null) {
            return hounslow;
          }
          if (hounslow !== null && mortlake !== null && hounslow < mortlake) {
            return mortlake;
          }

          // Weybridge (via Hounslow) service
          if (stop_code === 'WAT' && addlestone !== null) {
            return addlestone;
          }
          if (addlestone !== null && barnes !== null && addlestone < barnes) {
            return barnes;
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
            return slade_green;
          }
          if (slade_green !== null && eltham !== null && slade_green < eltham) {
            return eltham;
          }
          if (slade_green !== null && sidcup !== null && slade_green < sidcup) {
            return sidcup;
          }

          // rounder via Bexleyheath first
          if (bexleyheath !== null && eltham !== null && eltham < bexleyheath && dartford === null) {
            if (slade_green !== null && bexleyheath < slade_green) {
              return slade_green;
            }
            if (barnehurst !== null && bexleyheath < barnehurst) {
              return barnehurst;
            }
          }
          if (barnehurst !== null || stop_code === 'BNH') {
            if (woolwich !== null && (barnehurst ?? i) < woolwich) {
              return woolwich;
            }
            if (sidcup !== null && (barnehurst ?? i) < sidcup) {
              return sidcup;
            }
          }

          // rounder via Sidcup first
          if (sidcup !== null && dartford === null) {
            if (slade_green !== null && sidcup < slade_green) {
              return slade_green;
            }
            if (crayford !== null && sidcup < crayford) {
              return crayford;
            }
          }
          if (crayford !== null || stop_code === 'CRY') {
            if (woolwich !== null && (crayford ?? i) < woolwich) {
              return woolwich;
            }
            if (eltham !== null && (crayford ?? i) < eltham) {
              return eltham;
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
            return sandwich;
          }
          if (gravesend !== null && (stop_code === 'SDW' || (sandwich !== null && sandwich < gravesend))) {
            return gravesend;
          }

          // Kent coast rounder clockwise
          if (ramsgate !== null && gravesend !== null && gravesend < ramsgate) {
            return ramsgate;
          }
          if (folkestone_west !== null && margate !== null && margate < folkestone_west) {
            return folkestone_west;
          }
        }
        
        if (atoc_code === 'ME') {
          // Wirral line
          const moorfields = findCallingIndex('MRF');
          if (moorfields !== null) {
            const liverpool_central = findCallingIndex('LVC', moorfields);
            if (liverpool_central !== null) {
              if (findCallingIndex('LVJ', liverpool_central) !== null) {
                return liverpool_central;
              }
            }
          }
        }
        
        if (atoc_code === 'AW') {
          // Merthyr line
          const ninian_park = findCallingIndex('NNP', i);
          if (ninian_park !== null) {
            const cardiff_central = findCallingIndex('CDF', ninian_park);
            if (cardiff_central !== null) {
              const radyr = findCallingIndex('RDR', cardiff_central);
              if (radyr !== null) {
                return cardiff_central;
              }
            }
          }
        }

        {
          const huddersfield = findCallingIndex('HUD', i);
          if (huddersfield !== null) {
            const brighouse = findCallingIndex('BGH', huddersfield);
            if (brighouse !== null) {
              if (findCallingIndex('HUD', brighouse) /* again */) {
                return brighouse;
              }
            }
          }
        }
        return null;
      }

      const stop = stops[i];
      const stop_code = stop.stop_code ?? '';
      const false_destination_index = getFalseDestinationIndex();
      const false_destination = false_destination_index === null ? null : await repository.getStopName(stops[false_destination_index].stop_id);

      const via_tiplocs = stops.slice(i + 1, false_destination_index ?? -1)
          .filter(s => s.arrival_time !== null)
          .map(s => s.tiploc_code)

      const via = viaText[stop_code]?.reduce(
        // The wiki says that:
        // False destinations aren't considered for the purposes of determining the via text, 
        // but would still be displayed (e.g. at Leeds, the Leeds-York via Harrogate service would be displayed as "Poppleton via Harrogate").
        // 
        // however, it is not true in the real world. It is displayed as Poppleton only despite an entry of Leeds-York via Harrogate in the XML.
        (carry : ViaText[string][number] | null, item) => {
          const loc1index = via_tiplocs.indexOf(item.Loc1);
          const loc2index = item.Loc2 === null ? null : via_tiplocs.indexOf(item.Loc2);
          if (item.At === stop_code && item.Dest === (false_destination_index === null ? destination_tiploc : stops[false_destination_index].tiploc_code)
              && loc1index >= 0 && (item.Loc2 === null || loc2index! >= 0)
              && (item.Loc2 === null || loc2index! > loc1index)) {
            if (carry === null) {
              return item;
            }
            const carryIndex = via_tiplocs.indexOf(carry.Loc1);
            return loc1index < carryIndex ? item : carry;
          }
          return carry;
        },
        null,
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
