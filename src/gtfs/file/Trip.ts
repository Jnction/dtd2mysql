import {RSID} from "../native/OverlayRecord";

export enum Accessibility {
  UNKNOWN, YES, NO
}

export interface Trip {
  route_id: number;
  service_id: string;
  trip_id: string;
  trip_headsign: string | null;
  trip_short_name: RSID;
  direction_id: 0 | 1;
  block_id?: string;
  shape_id?: string;
  wheelchair_accessible: Accessibility;
  bikes_allowed: Accessibility;
  original_trip_id: string | null;
}