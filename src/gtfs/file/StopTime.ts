export interface StopTime {
  trip_id: number;
  arrival_time: string | null;
  departure_time: string | null;
  stop_id: StopPlatform;
  stop_sequence: number;
  stop_headsign: string | null;
  pickup_type: 0 | 1 | 2 | 3;
  drop_off_type: 0 | 1 | 2 | 3;
  shape_dist_traveled: null;
  timepoint: 0 | 1;
}

export type StopPlatform = string;