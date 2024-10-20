import {Platform} from './StopTime';

export interface Stop {
  stop_id: AtcoCode;
  stop_code: CRS;
  tiploc_code: string; // this is non-standard
  stop_name: string;
  stop_desc: string;
  stop_lat: number;
  stop_lon: number;
  zone_id: number;
  stop_url: string;
  location_type: 0 | 1;
  parent_station: AtcoCode;
  stop_timezone: string;
  wheelchair_boarding: 0 | 1 | 2;
  platform_code: Platform | null;
}

export type CRS = string;
export type TIPLOC = string;
export type AtcoCode = string;