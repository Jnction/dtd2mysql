import {ScheduleCalendar} from "./ScheduleCalendar";

export interface OverlayRecord {
  calendar: ScheduleCalendar;
  stp: STP;
  id: number;
  tuid: TUID;

  clone(calendar: ScheduleCalendar, scheduleId: number): this;
}


export type TUID = string;
export type RSID = string;

export enum STP {
  Permanent = "P",
  Overlay = "O",
  New = "N",
  Cancellation = "C"
}

export type IdGenerator = IterableIterator<number>;

