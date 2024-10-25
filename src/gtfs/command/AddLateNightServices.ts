import {Schedule} from "../native/Schedule";
import {IdGenerator, STP} from "../native/OverlayRecord";

/**
 * Loop through every schedule and replace any early morning services with a copy on the previous day.
 *
 * GTFS specification defines "time" as starting from noon minus 12 hours, which is normally midnight
 * but may be different by 1 hour on the day when the summer time zone changes, in order to avoid
 * a DST change happening inside a service day.
 *
 * Therefore, trains which depart before the change on changeover days should be recorded as on the
 * previous service day instead.
 */
export function addLateNightServices(schedules: Schedule[], idGenerator: IdGenerator): Schedule[] {
  const result: Schedule[] = [];

  for (const schedule of schedules) {
    // some trains start at an unadvertised stop - we assume that the departure hour is at the first public call
    const departureHour = parseInt(schedule.stopTimes.find(s => s.departure_time != null)!.departure_time!.substr(0, 2), 10);

    let isAnStpOvergroundServiceDepartingAtTheRepeatedHourAtAutumnClockChange =
        // the service is an STP service operated by London Overground    
        schedule.operator === 'LO' && schedule.stp === STP.New
        // which is only active for one day
        && schedule.calendar.runsFrom.isSame(schedule.calendar.runsTo)
        // and that day is the last Sunday of October
        && schedule.calendar.runsFrom.month() === 10 - 1 && schedule.calendar.runsFrom.date() >= 25 && schedule.calendar.runsFrom.weekday() === 0
        // and it departs between 01:00 and 01:59
        && departureHour === 1
    if (departureHour <= 1 && !isAnStpOvergroundServiceDepartingAtTheRepeatedHourAtAutumnClockChange) {
      const newSchedule = schedule.clone(schedule.calendar.shiftBackward(), idGenerator.next().value);

      for (const stop of newSchedule.stopTimes) {
        stop.departure_time = stop.departure_time === null ? null : parseInt(stop.departure_time.substr(0, 2), 10) + 24 + stop.departure_time.substr(2);
        stop.arrival_time = stop.arrival_time === null ? null : parseInt(stop.arrival_time.substr(0, 2), 10) + 24 + stop.arrival_time.substr(2);
      }

      result.push(newSchedule);
    } else {
      result.push(schedule);
    }
  }

  return result;
}