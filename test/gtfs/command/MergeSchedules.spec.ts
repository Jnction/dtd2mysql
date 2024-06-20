import moment = require("moment");
import {STP, TUID} from "../../../src/gtfs/native/OverlayRecord";
import {Days, ScheduleCalendar} from "../../../src/gtfs/native/ScheduleCalendar";
import {StopTime} from "../../../src/gtfs/file/StopTime";
import {Schedule} from "../../../src/gtfs/native/Schedule";
import {RouteType} from "../../../src/gtfs/file/Route";

const ALL_DAYS: Days = { 0: 1, 1: 1, 2: 1, 3: 1, 4: 1, 5: 1, 6: 1 };

export function schedule(id: number,
                         tuid: TUID,
                         from: string,
                         to: string,
                         stp: STP = STP.Overlay,
                         days: Days = ALL_DAYS,
                         stops: StopTime[] = []): Schedule {

  return new Schedule(
    id,
    `${tuid}_${moment(from).format('YYYYMMDD')}_${moment(to).format('YYYYMMDD')}`,
    stops,
    tuid,
    "",
    new ScheduleCalendar(
      moment(from),
      moment(to),
      days,
      {}
    ),
    RouteType.Rail,
    "LN",
    stp,
    true,
    true
  );
}
