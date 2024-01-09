import {Query} from 'mysql2';
import {getCrsFromStopId, Stop} from '../file/Stop';
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

  /**
   * Take a stream of ScheduleStopTimeRow, turn them into Schedule objects and add the result to the schedules
   */
  public loadSchedules(results: Query, stop_data : Stop[]): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      let stops: StopTime[] = [];
      let prevRow: ScheduleStopTimeRow;
      let departureHour = 4;

      results.on("result", (row: ScheduleStopTimeRow) => {
        if (prevRow && prevRow.id !== row.id) {
          const destination_crs = stops.length === 0 ? null : getCrsFromStopId(stops[stops.length - 1].stop_id);
          this.schedules.push(this.createSchedule(prevRow,
              stops,
              destination_crs === null ? null : CIFRepository.getStopNameFromStopData(stop_data, destination_crs) ?? destination_crs
          ));
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
        const destination_crs = stops.length === 0 ? null : getCrsFromStopId(stops[stops.length - 1].stop_id);
        this.schedules.push(this.createSchedule(prevRow, stops, destination_crs === null ? null : CIFRepository.getStopNameFromStopData(stop_data, destination_crs) ?? destination_crs));

        resolve();
      });
      results.on("error", reject);
    });
  }

  private createSchedule(row: ScheduleStopTimeRow, stops: StopTime[], destination_name : string | null): Schedule {
    this.maxId = Math.max(this.maxId, row.id);

    const mode = routeTypeIndex.hasOwnProperty(row.train_category) ? routeTypeIndex[row.train_category] : RouteType.Rail;

    this.fillStopHeadsigns(row.atoc_code, stops, destination_name);

    return new Schedule(
      row.id,
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

    const activities = row.activity.match(/.{1,2}/g) || [];
    const pickup = pickupActivities.find(a => activities.includes(a)) && !activities.includes(notAdvertised) ? 0 : 1;
    const coordinatedDropOff = coordinatedActivity.find(a => activities.includes(a)) ? 3 : 0;
    const dropOff = dropOffActivities.find(a => activities.includes(a)) ? 0 : 1;

    return {
      trip_id: row.id,
      arrival_time: (arrivalTime || departureTime),
      departure_time: (departureTime || arrivalTime),
      stop_id: `${row.crs_code}_${row.platform ?? ''}`,
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

  public fillStopHeadsigns(atoc_code : string | null, stops : StopTime[], destination_name : string | null) : void {
    if (stops.length === 0) return;

    const destination_code = stops[stops.length - 1].stop_id.substring(0, 3);
    function findCallingIndex(stop_code : string) : number | null {
      const result = stops.findIndex(stop => stop.stop_id.substring(0, 3) === stop_code);
      return result === -1 ? null : result;
    }
    // Sutton (via Mitcham Junction) / Sutton (via Wimbledon)
    if (stops[stops.length - 1].stop_id.substring(0, 3) === 'SUO' && atoc_code === 'TL') {
      const index = findCallingIndex('STE');
      const wimbledon = findCallingIndex('WIM');
      const mitcham = findCallingIndex('MIJ');
      if (index !== null) {
        for (let i = 0; i <= index; ++i) {
          stops[i].stop_headsign = wimbledon !== null ? 'Sutton (via Wimbledon)' : mitcham !== null ? 'Sutton (via Mitcham Junction)' : null;
        }
      }
    }

    if (stops[0].stop_id.substring(0, 3) === 'SUO' && atoc_code === 'TL') {
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

      for (let i = 0; i < stops.length; ++i) {
        const stop = stops[i];
        const stop_code = stop.stop_id.substring(0, 3);

        // Kingston loop clockwise
        if (['WAT', 'VXH', 'CLJ'].includes(stop_code) && wimbledon !== null && strawberry_hill !== null && i < wimbledon && wimbledon < strawberry_hill && staines === null) {
          stop.stop_headsign ??= 'Strawberry Hill (via Wimbledon)';
        }
        if (kingston !== null && richmond !== null && kingston < richmond && i < kingston) {
          stop.stop_headsign ??= 'Richmond';
        }
        if (kingston !== null && chiswick !== null && kingston < chiswick && i < kingston) {
          stop.stop_headsign ??= 'Chiswick';
        }

        // Kingston loop anti-clockwise
        if (['WAT', 'VXH', 'QRB', 'CLJ'].includes(stop_code) && strawberry_hill !== null && twickenham !== null && i < twickenham && twickenham < strawberry_hill) {
          const false_destination = teddington !== null ? 'Teddington' : destination_name;
          if (richmond !== null) {
            stop.stop_headsign ??= `${false_destination} (via Richmond)`;
          }
          if (hounslow !== null) {
            stop.stop_headsign ??= `${false_destination} (via Hounslow)`;
          }
        }
        if (wimbledon !== null && twickenham !== null && i <= twickenham && twickenham < wimbledon) {
          stop.stop_headsign ??= 'Wimbledon';
        }

        // Kingston loop between Kingston and Strawberry Hill
        if (['KNG', 'HMW', 'TED', 'STW', 'SHP', 'UPH', 'SUU', 'KMP', 'HMP', 'FLW'].includes(stop_code) && ['WAT', 'CLJ'].includes(destination_code)) {
          if (richmond !== null && i < richmond) {
            stop.stop_headsign ??= `${destination_name} (via Richmond)`;
          }
          if (hounslow !== null && i < hounslow) {
            stop.stop_headsign ??= `${destination_name} (via Hounslow)`;
          }
          if (wimbledon !== null && i < wimbledon) {
            stop.stop_headsign ??= `${destination_name} (via Wimbledon)`;
          }
        }

        // Hounslow loop clockwise
        if (hounslow !== null && richmond !== null && richmond < hounslow && i < richmond && ['WAT', 'VXH', 'QRB', 'CLJ', 'WNT', 'PUT', 'BNS'].includes(stop_code)) {
          stop.stop_headsign ??= 'Hounslow (via Richmond)';
        }
        if (twickenham !== null && chiswick !== null && twickenham < chiswick && i <= twickenham) {
          stop.stop_headsign ??= 'Chiswick';
        }
        if (stop_code === 'WTN' && barnes_bridge !== null && i < barnes_bridge) {
          stop.stop_headsign ??= 'Barnes Bridge';
        }

        // Hounslow loop anti-clockwise
        if (hounslow !== null && brentford !== null && brentford < hounslow && i < brentford && staines === null) {
          if (['WAT', 'VXH', 'QRB', 'CLJ', 'WNT', 'PUT', 'BNS'].includes(stop_code)) {
            stop.stop_headsign ??= 'Hounslow (via Brentford)';
          } else {
            stop.stop_headsign ??= 'Hounslow';
          }
        }
        if (hounslow !== null && mortlake !== null && i < hounslow && hounslow < mortlake) {
          stop.stop_headsign ??= 'Mortlake';
        }

        if (stop_code === 'HOU' && ['WAT', 'CLJ'].includes(destination_code)) {
          if (brentford !== null && i < brentford) {
            stop.stop_headsign ??= `${destination_name} (via Brentford)`;
          }
          if (richmond !== null && i < richmond) {
            stop.stop_headsign ??= `${destination_name} (via Richmond)`;
          }
        }

        // Weybridge (via Hounslow) service
        if (i === 0 && stop_code === 'WAT' && addlestone !== null) {
          stop.stop_headsign ??= 'Addlestone';
        }
        if (['WYB', 'WOK'].includes(destination_code) && staines !== null && ['VXH', 'QRB', 'CLJ'].includes(stop_code)) {
          stop.stop_headsign ??= `${destination_name} (via Staines)`;
        }
        if (addlestone !== null && barnes !== null && addlestone < barnes && i < addlestone) {
          stop.stop_headsign ??= 'Barnes';
        }

        // Waterloo - Guildford services
        if (
            ['GLD', 'EFF'].includes(destination_code) && ['WAT', 'VXH', 'CLJ', 'EAD', 'WIM', 'RAY', 'NEM', 'BRS', 'SUR'].includes(stop_code)
            || (stop_code === 'GLD' || effingham_junction !== null && i <= effingham_junction) && ['CLJ', 'WAT'].includes(destination_code)
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
        if (['FTN', 'PMS', 'PMH'].includes(destination_code)) {
          if (guildford !== null && i < guildford && stop_code !== 'WPL') {
            stop.stop_headsign ??= `${destination_name} (via Guildford)`;
          }
          // the NRE app shows via Basingstoke but the PIDS shows via Winchester
          if (winchester !== null && (woking !== null && i < woking || ['WAT', 'CLJ'].includes(stop_code))) {
            stop.stop_headsign ??= `${destination_name} (via Winchester)`;
          }
        }
        if (['FTN', 'PMS', 'PMH'].includes(stop_code) && ['WOK', 'SUR', 'WIM', 'CLJ', 'WAT'].includes(destination_code)) {
          if (guildford !== null) stop.stop_headsign ??= `${destination_name} (via Guildford)`;
          if (winchester !== null) stop.stop_headsign ??= `${destination_name} (via Winchester)`;
        }
      }
    }

    if (atoc_code === 'LO') {
      if (destination_code === 'CLJ') {
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
      if (destination_code === 'HHY') {
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
