import {TIPLOC} from "../file/Stop";
import {StopTime} from "../file/StopTime";
import {formatDuration} from "./Duration";
import {IdGenerator, OverlayRecord, STP, TUID} from "./OverlayRecord";
import {Schedule} from "./Schedule";
import {NO_DAYS, ScheduleCalendar} from "./ScheduleCalendar";
import moment = require("moment");

export class Association implements OverlayRecord {

  constructor(
    public readonly id: number,
    public readonly baseTUID: TUID,
    public readonly assocTUID: TUID,
    public readonly assocLocation: TIPLOC,
    public readonly dateIndicator: DateIndicator,
    public readonly assocType: AssociationType,
    public readonly calendar: ScheduleCalendar,
    public readonly stp: STP
  ) { }

  public get tuid(): TUID {
    return this.baseTUID + "_" + this.assocTUID + "_";
  }

  /**
   * Clone the association with a different calendar
   */
  public clone(calendar: ScheduleCalendar, id: number): this {
    return new Association(
      id,
      this.baseTUID,
      this.assocTUID,
      this.assocLocation,
      this.dateIndicator,
      this.assocType,
      calendar,
      this.stp
    ) as this;
  }

  /**
   * Apply the join or split to the associated schedule. Check for any days that the associated service runs but the
   * association does not and create additional schedules to cover those periods.
   */
  public apply(base: Schedule, assoc: Schedule, idGenerator: IdGenerator): Schedule[] {
    const assocCalendar = this.dateIndicator === DateIndicator.Next ? this.calendar.shiftForward() : 
        this.dateIndicator === DateIndicator.Previous ? this.calendar.shiftBackward() : this.calendar;
    const mergedBase = this.mergeSchedules(base, assoc);
    const schedules = mergedBase !== null ? [mergedBase] : [];

    // exclude the associated schedule from running when the association is active
    const excludeCalendar = assoc.calendar.addExcludeDays(assocCalendar);
    if (excludeCalendar !== null) {
      schedules.push(assoc.clone(excludeCalendar, idGenerator.next().value));
    }

    return schedules;
  }

  /**
   * Apply the split or join to the given schedules
   */
  private mergeSchedules(base: Schedule, assoc: Schedule): Schedule | null {
    let tuid: TUID;
    let start: StopTime[];
    let assocStop: StopTime;
    let end: StopTime[];

    const baseStopTime = base.stopAt(this.assocLocation);
    const assocStopTime = assoc.stopAt(this.assocLocation);

    // this should never happen, unless data feed is corrupted. It will prevent us from update failure
    if (baseStopTime === undefined || assocStopTime === undefined) {
      return assoc;
    }

    if (this.assocType === AssociationType.Split) {
      tuid = base.tuid + "_" + assoc.tuid;

      start = base.before(this.assocLocation);
      assocStop = this.mergeAssociationStop(baseStopTime, assocStopTime);
      end = assoc.after(this.assocLocation);
    }
    else {
      tuid = assoc.tuid + "_" + base.tuid;

      start = assoc.before(this.assocLocation);
      assocStop = this.mergeAssociationStop(assocStopTime, baseStopTime);
      end = base.after(this.assocLocation)
    }

    let stopSequence: number = 1;
    const calendar = this.dateIndicator === DateIndicator.Next ? assoc.calendar.shiftBackward() : assoc.calendar;
    const thisCalendar = this.dateIndicator === DateIndicator.Previous ? this.calendar.shiftBackward() : this.calendar; 

    const newCalendar = calendar.clone(
        moment.max(thisCalendar.runsFrom, calendar.runsFrom),
        moment.min(thisCalendar.runsTo, calendar.runsTo),
        NO_DAYS,
        {...calendar.excludeDays, ...thisCalendar.excludeDays}
    );
    if (newCalendar === null) return null;
    const tripId = `${tuid}_${moment(newCalendar.runsFrom).format('YYYYMMDD')}_${moment(newCalendar.runsTo).format('YYYYMMDD')}`;

    const stops = [
      ...start.map(s => cloneStop(s, stopSequence++, tripId, false, false, this.assocType === AssociationType.Split)),
      cloneStop(assocStop, stopSequence++, tripId, false, this.assocType === AssociationType.Join, this.assocType === AssociationType.Split),
      ...end.map(s => cloneStop(s, stopSequence++, tripId, this.assocType === AssociationType.Split && this.dateIndicator === DateIndicator.Next || this.assocType === AssociationType.Join && this.dateIndicator === DateIndicator.Previous, this.assocType === AssociationType.Join, false))
    ];

    return new Schedule(
      assoc.id,
      tripId,
      stops,
      tuid,
      assoc.rsid,
      // only take the part of the schedule that the association applies to
      newCalendar,
      assoc.mode,
      assoc.operator,
      assoc.stp,
      assoc.firstClassAvailable,
      assoc.reservationPossible
    )
  }

  /**
   * Take the arrival time of the first stop and the departure time of the second stop and put them into a new stop
   */
  public mergeAssociationStop(arrivalStop: StopTime, departureStop: StopTime): StopTime {
    let arrivalTime = arrivalStop.arrival_time === null ? null : moment.duration(arrivalStop.arrival_time);
    let departureTime = departureStop.departure_time === null ? null : moment.duration(departureStop.departure_time);

    if (this.dateIndicator !== DateIndicator.Same) {
      departureTime?.add(1, "days");
    }

    arrivalTime = arrivalTime || departureTime;
    departureTime = departureTime || arrivalTime;
    
    return Object.assign({}, arrivalStop, {
      arrival_time: arrivalTime === null ? null : formatDuration(arrivalTime.asSeconds()),
      departure_time: departureTime === null ? null : formatDuration(departureTime.asSeconds()),
      pickup_type: departureStop.pickup_type,
      drop_off_type: arrivalStop.drop_off_type
    });
  }

}

/**
 * Clone the given stop overriding the sequence number and modifying the arrival/departure times if necessary
 */
function cloneStop(
    stop : StopTime,
    stopSequence : number,
    tripId : string,
    nextDay : boolean,
    disablePickup : boolean = false,
    disableDropOff : boolean = false
): StopTime {
  const departureTime = stop.departure_time ? moment.duration(stop.departure_time) : null;
  if (nextDay) departureTime?.add(1, "day");

  const arrivalTime = stop.arrival_time ? moment.duration(stop.arrival_time) : null;
  if (nextDay) arrivalTime?.add(1, "day");

  let override = disablePickup ? {pickup_type : 1} : {};
  if (disableDropOff) {
    override = Object.assign(override, {drop_off_type : 1});
  }

  return Object.assign({}, stop, override, {
    arrival_time: arrivalTime ? formatDuration(arrivalTime.asSeconds()) : null,
    departure_time: departureTime ? formatDuration(departureTime.asSeconds()) : null,
    stop_sequence: stopSequence,
    trip_id: tripId
  });
}

export enum DateIndicator {
  Same = "S",
  Next = "N",
  Previous = "P"
}

export enum AssociationType {
  Split = "VV",
  Join = "JJ",
  NA = ""
}
