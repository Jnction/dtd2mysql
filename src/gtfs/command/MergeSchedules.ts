import {Schedule} from "../native/Schedule";
import {ScheduleIndex} from './ApplyAssociations';

/**
 * Flatten the index into a list of schedules, ensuring that there are no duplicate trip IDs
 */
export function mergeSchedules(schedulesByTuid: ScheduleIndex): Schedule[] {
  const schedulesByTripId: {[tripId : string]: Schedule} = {};

  for (const tuid in schedulesByTuid) {
    if (schedulesByTuid.hasOwnProperty(tuid)) {
      for (const schedule of schedulesByTuid[tuid]) {
        // if the service has no public calls, skip processing
        if (schedule.stopTimes.find(item => item.departure_time !== null || item.arrival_time !== null) === undefined) {
          continue;
        }

        const tripId = schedule.tripId;
        if (schedulesByTripId[tripId] !== undefined) {
          throw new Error(`Duplicate trip_id ${tripId} detected. This should not happen. Please file a bug report.`);
        }
        schedulesByTripId[tripId] = schedule;
      }
    }
  }

  return Object.values(schedulesByTripId);
}