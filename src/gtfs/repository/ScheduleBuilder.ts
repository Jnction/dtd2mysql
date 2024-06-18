import {Query} from 'mysql2';
import {CRS} from '../file/Stop';
import {IdGenerator, STP} from "../native/OverlayRecord";
import {Schedule} from "../native/Schedule";
import {RouteType} from "../file/Route";
import moment = require("moment");
import {ScheduleCalendar} from "../native/ScheduleCalendar";
import {CIFRepository, ScheduleStopTimeRow} from "./CIFRepository";
import {StopTime} from "../file/StopTime";

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
    let arrivalTime, departureTime;

    // if either public time is set, use those
    if (row.public_arrival_time || row.public_departure_time) {
      arrivalTime = this.formatTime(row.public_arrival_time, departHour);
      departureTime = this.formatTime(row.public_departure_time, departHour);
    }
    // if no public time at all (no set down or pick) use the scheduled time
    else {
      arrivalTime = this.formatTime(row.scheduled_arrival_time, departHour);
      departureTime = this.formatTime(row.scheduled_departure_time, departHour);
    }

    const activities = (row.activity ?? '').match(/.{1,2}/g) || [] as string[];
    const pickup = pickupActivities.find(a => activities.includes(a)) && !activities.includes(notAdvertised) ? 0 : 1;
    const coordinatedDropOff = coordinatedActivity.find(a => activities.includes(a)) ? 3 : 0;
    const dropOff = dropOffActivities.find(a => activities.includes(a)) ? 0 : 1;

    return {
      trip_id: this.getTripId(row),
      arrival_time: (arrivalTime || departureTime),
      departure_time: (departureTime || arrivalTime),
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
    const atoc_code = schedule.operator;
    const stops = schedule.stopTimes;
    if (stops.length === 0) return;

    const destination_id = stops[stops.length - 1].stop_id;
    const destination_name = await repository.getStopName(destination_id);
    const stations = await repository.getStops();

    for (let i = 0; i < stops.length; ++i) {
      const stop = stops[i];
      const stop_code = stop.stop_code ?? '';

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
      // Sutton (via Mitcham Junction) / Sutton (via Wimbledon)
      if (stops[stops.length - 1].stop_code === 'SUO' && atoc_code === 'TL') {
        const index = findCallingIndex('STE', i);
        const wimbledon = findCallingIndex('WIM');
        const mitcham = findCallingIndex('MIJ');
        if (index !== null) {
          stops[i].stop_headsign = wimbledon !== null ? 'Sutton (via Wimbledon)' : mitcham !== null ? 'Sutton (via Mitcham Junction)' : null;
        }
      }

      if (stop_code === 'SUO' && atoc_code === 'TL') {
        const wimbledon = findCallingIndex('WIM');
        const mitcham = findCallingIndex('MIJ');
        stops[0].stop_headsign = wimbledon !== null ? `${destination_name} (via Wimbledon)` : mitcham !== null ? `${destination_name} (via Mitcham Junction)` : null;
      }

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

        const effingham_junction = findCallingIndex('EFF');
        const cobham = findCallingIndex('CSD');
        const epsom = findCallingIndex('EPS');
        const woking = findCallingIndex('WOK');

        const winchester = findCallingIndex('WIN');
        const guildford = findCallingIndex('GLD');
        const basingstoke = findCallingIndex('BSK');

        // Kingston loop clockwise
        if (['WAT', 'VXH', 'CLJ'].includes(stop_code) && wimbledon !== null && strawberry_hill !== null && wimbledon < strawberry_hill && staines === null) {
          stop.stop_headsign ??= 'Strawberry Hill (via Wimbledon)';
        }
        if (kingston !== null && richmond !== null && kingston < richmond) {
          stop.stop_headsign ??= 'Richmond';
        }
        if (kingston !== null && chiswick !== null && kingston < chiswick) {
          stop.stop_headsign ??= 'Chiswick';
        }

        // Kingston loop anti-clockwise
        if (['WAT', 'VXH', 'QRB', 'CLJ'].includes(stop_code) && strawberry_hill !== null && twickenham !== null && twickenham < strawberry_hill) {
          const false_destination = teddington !== null ? 'Teddington' : destination_name;
          if (richmond !== null) {
            stop.stop_headsign ??= `${false_destination} (via Richmond)`;
          }
          if (hounslow !== null) {
            stop.stop_headsign ??= `${false_destination} (via Hounslow)`;
          }
        }
        if (wimbledon !== null && (twickenham !== null && twickenham < wimbledon || stop_code === 'TWI')) {
          stop.stop_headsign ??= 'Wimbledon';
        }

        // Kingston loop between Kingston and Strawberry Hill
        if (['KNG', 'HMW', 'TED', 'STW', 'SHP', 'UPH', 'SUU', 'KMP', 'HMP', 'FLW'].includes(stop_code) && ['WAT', 'CLJ'].includes(destination_id)) {
          if (richmond !== null) {
            stop.stop_headsign ??= `${destination_name} (via Richmond)`;
          }
          if (hounslow !== null) {
            stop.stop_headsign ??= `${destination_name} (via Hounslow)`;
          }
          if (wimbledon !== null) {
            stop.stop_headsign ??= `${destination_name} (via Wimbledon)`;
          }
        }

        // Hounslow loop clockwise
        if (hounslow !== null && richmond !== null && richmond < hounslow && ['WAT', 'VXH', 'QRB', 'CLJ', 'WNT', 'PUT', 'BNS'].includes(stop_code)) {
          stop.stop_headsign ??= 'Hounslow (via Richmond)';
        }
        if (chiswick !== null && (stop_code === 'TWI' || twickenham !== null && twickenham < chiswick)) {
          stop.stop_headsign ??= 'Chiswick';
        }
        if (stop_code === 'WTN' && barnes_bridge !== null) {
          stop.stop_headsign ??= 'Barnes Bridge';
        }

        // Hounslow loop anti-clockwise
        if (hounslow !== null && brentford !== null && brentford < hounslow && staines === null) {
          if (['WAT', 'VXH', 'QRB', 'CLJ', 'WNT', 'PUT', 'BNS'].includes(stop_code)) {
            stop.stop_headsign ??= 'Hounslow (via Brentford)';
          } else {
            stop.stop_headsign ??= 'Hounslow';
          }
        }
        if (hounslow !== null && mortlake !== null && hounslow < mortlake) {
          stop.stop_headsign ??= 'Mortlake';
        }

        if (stop_code === 'HOU' && ['WAT', 'CLJ'].includes(destination_id)) {
          if (brentford !== null) {
            stop.stop_headsign ??= `${destination_name} (via Brentford)`;
          }
          if (richmond !== null) {
            stop.stop_headsign ??= `${destination_name} (via Richmond)`;
          }
        }

        // Weybridge (via Hounslow) service
        if (stop_code === 'WAT' && addlestone !== null) {
          stop.stop_headsign ??= 'Addlestone';
        }
        if (['WYB', 'WOK'].includes(destination_id) && staines !== null && ['VXH', 'QRB', 'CLJ'].includes(stop_code)) {
          stop.stop_headsign ??= `${destination_name} (via Staines)`;
        }
        if (addlestone !== null && barnes !== null && addlestone < barnes) {
          stop.stop_headsign ??= 'Barnes';
        }

        // Waterloo - Guildford services
        if (
            ['GLD', 'EFF'].includes(destination_id) && ['WAT', 'VXH', 'CLJ', 'EAD', 'WIM', 'RAY', 'NEM', 'BRS', 'SUR'].includes(stop_code)
            || (stop_code === 'GLD' || effingham_junction !== null && i <= effingham_junction) && ['CLJ', 'WAT'].includes(destination_id)
        ) {
          if (woking !== null) {
            stop.stop_headsign ??= `${destination_name} (via Woking)`;
          }
          if (cobham !== null) {
            stop.stop_headsign ??= `${destination_name} (via Cobham)`;
          }
          if (epsom !== null) {
            stop.stop_headsign ??= `${destination_name} (via Epsom)`;
          }
        }

        // Waterloo - Portsmouth services
        if (['FTN', 'PMS', 'PMH'].includes(destination_id)) {
          if (guildford !== null && stop_code !== 'WPL') {
            stop.stop_headsign ??= `${destination_name} (via Guildford)`;
          }
          // the NRE app shows via Basingstoke but the PIDS shows via Winchester
          if (winchester !== null && (woking !== null || ['WAT', 'CLJ'].includes(stop_code))) {
            stop.stop_headsign ??= `${destination_name} (via Winchester)`;
          }
        }
        if (['FTN', 'PMS', 'PMH'].includes(stop_code) && ['WOK', 'SUR', 'WIM', 'CLJ', 'WAT'].includes(destination_id)) {
          if (guildford !== null) stop.stop_headsign ??= `${destination_name} (via Guildford)`;
          if (winchester !== null) stop.stop_headsign ??= `${destination_name} (via Winchester)`;
        }
      }

      if (atoc_code === 'SE') {
        const dartford = findCallingIndex('DFD');
        const woolwich = findCallingIndex('WWA');
        const bexleyheath = findCallingIndex('BXH');
        const sidcup = findCallingIndex('SID');
        const lewisham = findCallingIndex('LEW');
        const greenwich = findCallingIndex('GNW');
        const slade_green = findCallingIndex('SGR');
        const eltham = findCallingIndex('ELW');
        const crayford = findCallingIndex('CRY');
        const barnehurst = findCallingIndex('BNH');
        const charlton = findCallingIndex('CTN');
        const hither_green = findCallingIndex('HGR');
        const ladywell = findCallingIndex('LAD');

        // 3 routes between London Bridge and Dartford
        if (dartford !== null || stop_code === 'DFD') {
          if (['CHX', 'LBG', 'CST', 'VIC', 'BFR'].includes(destination_id)) {
            if (sidcup !== null) {
              if (lewisham !== null) {
                stop.stop_headsign ??= `${destination_name} (via Sidcup & Lewisham)`;
              }
              stop.stop_headsign ??= `${destination_name} (via Sidcup)`;
            }
            if (bexleyheath !== null) {
              stop.stop_headsign ??= `${destination_name} (via Bexleyheath)`;
            }
            if (woolwich !== null) {
              if (greenwich !== null) {
                stop.stop_headsign ??= `${destination_name} (via Woolwich & Greenwich)`;
              } else if (lewisham !== null) {
                stop.stop_headsign ??= `${destination_name} (via Woolwich & Lewisham)`;
              } else {
                stop.stop_headsign ??= `${destination_name} (via Woolwich)`;
              }
            }
          }
          if (['CHX', 'WAE', 'LBG', 'CST', 'NWX', 'SAJ'].includes(stop_code)) {
            if (sidcup !== null) {
              if (lewisham !== null) {
                stop.stop_headsign ??= `${destination_name} (via Lewisham & Sidcup)`;
              }
              stop.stop_headsign ??= `${destination_name} (via Sidcup)`;
            }
            if (bexleyheath !== null) {
              stop.stop_headsign ??= `${destination_name} (via Bexleyheath)`;
            }
            if (woolwich !== null) {
              if (greenwich !== null) {
                stop.stop_headsign ??= `${destination_name} (via Greenwich & Woolwich)`;
              } else if (lewisham !== null) {
                stop.stop_headsign ??= `${destination_name} (via Lewisham & Woolwich)`;
              } else {
                stop.stop_headsign ??= `${destination_name} (via Woolwich)`;
              }
            }
          }
          if (dartford !== null && (lewisham !== null && lewisham < dartford || ['LEW', 'BKH'].includes(stop_code))) {
            if (sidcup !== null) {
              stop.stop_headsign ??= `${destination_name} (via Sidcup)`;
            }
            if (bexleyheath !== null) {
              stop.stop_headsign ??= `${destination_name} (via Bexleyheath)`;
            }
            if (woolwich !== null) {
              stop.stop_headsign ??= `${destination_name} (via Woolwich)`;
            }
          }
        }

        // rounder via Woolwich first
        if (woolwich !== null && slade_green !== null && woolwich < slade_green && dartford === null) {
          if (['CHX', 'WAE', 'LBG', 'CST'].includes(stop_code)) {
            if (greenwich !== null) {
              stop.stop_headsign ??= `Slade Green (via Greenwich & Woolwich)`;
            }
            if (lewisham !== null) {
              stop.stop_headsign ??= `Slade Green (via Lewisham & Woolwich)`;
            }
          }
          if (lewisham !== null && lewisham < woolwich || ['LEW', 'BKH'].includes(stop_code)) {
            stop.stop_headsign ??= 'Slade Green (via Woolwich)';
          }
          stop.stop_headsign ??= 'Slade Green';
        }
        if (slade_green !== null && eltham !== null && slade_green < eltham) {
          stop.stop_headsign ??= 'Eltham';
        }
        if (slade_green !== null && sidcup !== null && slade_green < sidcup) {
          stop.stop_headsign ??= 'Sidcup';
        }
        if (stop_code === 'SGR') {
          if (bexleyheath !== null) {
            stop.stop_headsign ??= `${destination_name} (via Bexleyheath)`;
          }
          if (sidcup !== null) {
            stop.stop_headsign ??= `${destination_name} (via Sidcup)`;
          }
        }

        // rounder via Bexleyheath first
        if (bexleyheath !== null && eltham !== null && eltham < bexleyheath && dartford === null) {
          const viaString = stop_code === 'KDB' ? '' : ' (via Bexleyheath)';
          if (slade_green !== null && bexleyheath < slade_green) {
            stop.stop_headsign ??= `Slade Green${viaString}`;
          }
          if (barnehurst !== null && bexleyheath < barnehurst) {
            stop.stop_headsign ??= `Barnehurst${viaString}`;
          }
        }
        if (barnehurst !== null || stop_code === 'BNH') {
          if (woolwich !== null && (barnehurst ?? i) < woolwich) {
            stop.stop_headsign ??= 'Woolwich Arsenal';
          }
          if (sidcup !== null && (barnehurst ?? i) < sidcup) {
            stop.stop_headsign ??= 'Sidcup';
          }
        }

        // rounder via Sidcup first
        if (sidcup !== null && dartford === null) {
          const viaString =
              ['CHX', 'WAE', 'LBG', 'CST', 'NWX', 'SAJ'].includes(stop_code)
                ? lewisham !== null && lewisham < sidcup ? ' (via Lewisham & Sidcup)' : ' (via Sidcup)'
                : lewisham !== null && lewisham < sidcup || stop_code === 'LEW' ? ' (via Sidcup)' : '';
          if (slade_green !== null && sidcup < slade_green) {
            stop.stop_headsign ??= `Slade Green${viaString}`;
          }
          if (crayford !== null && sidcup < crayford) {
            stop.stop_headsign ??= `Crayford${viaString}`;
          }
        }
        if (crayford !== null || stop_code === 'CRY') {
          if (woolwich !== null && (crayford ?? i) < woolwich) {
            stop.stop_headsign ??= 'Woolwich Arsenal';
          }
          if (eltham !== null && (crayford ?? i) < eltham) {
            stop.stop_headsign ??= 'Eltham';
          }
        }

        // Woolwich line westbound via Greenwich or Lewisham
        if ((charlton !== null || stop_code === 'CTN') && ['LBG', 'CST', 'CHX'].includes(destination_id)) {
          const lewisham_after_charlton = findCallingIndex('LEW', (charlton ?? i) + 1);
          if (greenwich !== null && (charlton ?? i) < greenwich) {
            stop.stop_headsign ??= `${destination_name} (via Greenwich)`;
          }
          if (lewisham_after_charlton !== null) {
            stop.stop_headsign ??= `${destination_name} (via Lewisham)`;
          }
        }

        // Sidcup line or Hayes line via Lewisham
        const fork = hither_green ?? ladywell;
        const at_or_before_fork = fork !== null || ['HGR', 'LAD'].includes(stop_code);
        if (['LBG', 'CST', 'CHX'].includes(destination_id)) {
          if (at_or_before_fork && findCallingIndex('LEW', fork ?? i) !== null) {
            stop.stop_headsign ??= `${destination_name} (via Lewisham)`;
          }
        }
        if (at_or_before_fork && lewisham !== null && lewisham < (fork ?? i)) {
          stop.stop_headsign ??= `${destination_name} (via Lewisham)`;
        }

        const ashford = findCallingIndex('AFK');
        const sandwich = findCallingIndex('SDW');
        const gravesend = findCallingIndex('GRV');
        const ramsgate = findCallingIndex('RAM');
        const folkestone_west = findCallingIndex('FKW');
        const margate = findCallingIndex('MAR');
        const chatham = findCallingIndex('CTM');
        const dover = findCallingIndex('DVP');
        const canterbury_west = findCallingIndex('CBW');
        // Kent coast rounder anti clockwise
        if (sandwich !== null && (ashford !== null && ashford < sandwich || stop_code === 'AFK')
            && canterbury_west === null /* https://github.com/planarnetwork/dtd2mysql/issues/80 */
        ) {
          stop.stop_headsign ??= 'Sandwich';
        }
        if (gravesend !== null && (stop_code === 'SDW' || (sandwich !== null && sandwich < gravesend))) {
          stop.stop_headsign ??= 'Gravesend';
        }

        // Kent coast rounder clockwise
        if (ramsgate !== null && gravesend !== null && gravesend < ramsgate) {
          stop.stop_headsign ??= 'Ramsgate';
        }
        if (folkestone_west !== null && margate !== null && margate < folkestone_west) {
          stop.stop_headsign ??= 'Folkestone West';
        }

        if (chatham !== null && (stop_code === 'MAR' || margate !== null && margate < chatham)) {
          stop.stop_headsign ??= `${destination_name} (via Chatham)`;
        }
        if (sandwich !== null && dover !== null && ashford !== null && dover < ashford && sandwich < dover) {
          stop.stop_headsign ??= `${destination_name} (via Dover Priory)`;
        }
        if (canterbury_west !== null && (
            ashford !== null && (canterbury_west < ashford && !['MSR', 'STU'].includes(stop_code))
            || ramsgate !== null && (canterbury_west < ramsgate && !['WYE', 'CIL', 'CRT'].includes(stop_code))
        )) {
          stop.stop_headsign ??= `${destination_name} (via Canterbury West)`;
        }
      }

      if (atoc_code === 'LO') {
        if (destination_id === 'CLJ') {
          for (const stop of stops) {
            if (['HHY', 'CNN'].includes(stop.stop_id.substring(0, 3))) {
              if (findCallingIndex('WIJ')) {
                stop.stop_headsign = 'Clapham Junction (via Willesden Junction)';
              }
              if (findCallingIndex('SQE')) {
                stop.stop_headsign = 'Clapham Junction (via Surrey Quays)';
              }
            }
          }
        }
        if (destination_id === 'HHY') {
          for (const stop of stops) {
            if (stop.stop_id.substring(0, 3) === 'CLJ') {
              if (findCallingIndex('WIJ')) {
                stop.stop_headsign = 'Highbury & Islington (via Willesden Junction)';
              }
              if (findCallingIndex('SQE')) {
                stop.stop_headsign = 'Highbury & Islington (via Surrey Quays)';
              }
            }
          }
        }
      }
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
