import {Duration} from "../native/Duration";
import {AtcoCode} from './Stop';

/**
 * A transfer may be interchange at a particular station (where the fromStopId and toStopId are the same) or a fixed
 * leg between two different stations (a walk or tube).
 */
export interface Transfer {
  from_stop_id: AtcoCode,
  to_stop_id: AtcoCode,
  transfer_type: TransferType,
  min_transfer_time: Duration
}

export enum TransferType {
  Recommended = 0,
  Timed = 1,
  MinTime = 2,
  NotPossible = 3
}

