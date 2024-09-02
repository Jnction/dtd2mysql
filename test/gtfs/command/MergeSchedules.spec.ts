import moment = require("moment");
import {AtcoCode, CRS, TIPLOC} from '../../../src/gtfs/file/Stop';
import {STP, TUID} from "../../../src/gtfs/native/OverlayRecord";
import {Days, ScheduleCalendar} from "../../../src/gtfs/native/ScheduleCalendar";
import {StopTime} from "../../../src/gtfs/file/StopTime";
import {Schedule} from "../../../src/gtfs/native/Schedule";
import {RouteType} from "../../../src/gtfs/file/Route";

const ALL_DAYS: Days = { 0: 1, 1: 1, 2: 1, 3: 1, 4: 1, 5: 1, 6: 1 };

const DEFAULT_STOP_TIMES : StopTime[] = [
  {
    trip_id: 'LN1234',
    arrival_time: '12:30',
    departure_time: '12:30',
    stop_id: '9100XXXXXX1',
    stop_code: 'XXX',
    tiploc_code: 'XXXXXX',
    stop_sequence: 0,
    stop_headsign: null,
    pickup_type: 0,
    drop_off_type: 0,
    shape_dist_traveled: null,
    timepoint: 1,
  },
  {
    trip_id: 'LN1234',
    arrival_time: '13:00',
    departure_time: '13:00',
    stop_id: '9100YYYYYY1',
    stop_code: 'YYY',
    tiploc_code: 'YYYYYY',
    stop_sequence: 1,
    stop_headsign: null,
    pickup_type: 0,
    drop_off_type: 0,
    shape_dist_traveled: null,
    timepoint: 1,
  },
]

export function schedule(id: number,
                         tuid: TUID,
                         from: string,
                         to: string,
                         stp: STP = STP.Overlay,
                         days: Days = ALL_DAYS,
                         stops: StopTime[] = DEFAULT_STOP_TIMES): Schedule {

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
