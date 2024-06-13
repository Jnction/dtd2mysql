import {AtcoCode, CRS, TIPLOC} from './Stop';

export interface StopTime {
  trip_id: string;
  arrival_time: string | null;
  departure_time: string | null;
  stop_id: AtcoCode;
  stop_code: CRS;
  tiploc_code: TIPLOC;
  stop_sequence: number;
  stop_headsign: string | null;
  pickup_type: 0 | 1 | 2 | 3;
  drop_off_type: 0 | 1 | 2 | 3;
  shape_dist_traveled: null;
  timepoint: 0 | 1;
}

export type Platform = string;