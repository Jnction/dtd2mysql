import  objectHash = require('object-hash');
import {agencies} from '../../../config/gtfs/agency';
import {AgencyID} from "../file/Agency";
import {Route, RouteID, RouteType} from "../file/Route";
import {Shape} from '../file/Shape';
import {AtcoCode, CRS, TIPLOC} from "../file/Stop";
import {StopTime} from "../file/StopTime";
import {Trip} from "../file/Trip";
import {CIFRepository} from '../repository/CIFRepository';
import {OverlayRecord, RSID, STP, TUID} from "./OverlayRecord";
import {ScheduleCalendar} from "./ScheduleCalendar";

/**
 * A CIF schedule (BS record)
 */
export class Schedule implements OverlayRecord {

  constructor(
    public readonly id: number,
    public readonly tripId: string,
    public readonly stopTimes: StopTime[],
    public readonly tuid: TUID,
    public readonly rsid: RSID | null,
    public readonly calendar: ScheduleCalendar,
    public readonly mode: RouteType,
    public readonly operator: string | null,
    public readonly stp: STP,
    public readonly firstClassAvailable: boolean,
    public readonly reservationPossible: boolean
  ) {}

  public get origin(): AtcoCode {
    return this.stopTimes[0].stop_id;
  }

  public get destination(): AtcoCode {
    return this.stopTimes[this.stopTimes.length - 1].stop_id;
  }

  /**
   * Clone the current record with the new calendar and id
   */
  public clone(calendar: ScheduleCalendar, scheduleId: number): this {
    return new Schedule(
      scheduleId,
      this.tripId,
      this.stopTimes,
      this.tuid,
      this.rsid,
      calendar,
      this.mode,
      this.operator,
      this.stp,
      this.firstClassAvailable,
      this.reservationPossible
    ) as this;
  }

  /**
   * Convert to a GTFS Trip
   */
  public async toTrip(serviceId: string, routeId: number, cifRepository : CIFRepository): Promise<Trip> {
    return {
      route_id: routeId,
      service_id: serviceId,
      trip_id: this.tripId,
      trip_headsign: await cifRepository.getStopName(this.destination) ?? this.destination,
      trip_short_name: this.rsid?.substr(0, 6) ?? this.tuid,
      direction_id: 0,
      shape_id: this.getShapeId(),
      wheelchair_accessible: 1,
      bikes_allowed: 0,
      original_trip_id: this.tuid,
    };
  }

  /**
   * Convert to GTFS Shape Points
   */
  public async toShape(cifRepository : CIFRepository): Promise<Shape[]> {
    const result : Shape[] = [];
    let sequence = 0;
    for (const stopTime of this.stopTimes) {
      const entry = await cifRepository.findStopById(stopTime.stop_id)
          ?? cifRepository.tiplocCoordinates[stopTime.stop_id.replace(/^9100/, '')];
      if (entry !== undefined && entry.stop_lat !== null && entry.stop_lon !== null) {
        result.push({
          shape_id: this.getShapeId(),
          shape_pt_lat: entry.stop_lat,
          shape_pt_lon: entry.stop_lon,
          shape_pt_sequence: sequence++,
        });
      }
    }
    return result;
  }

  public getShapeId(): string {
    return objectHash(this.stopTimes.map(stopTime => stopTime.stop_id).join('-'));
  }

  public getNameAndColour(routeLongName : string) : {route_id : RouteID, name : string, long_name? : string, colour : number | null} {
    const result = this.getNameAndColourWithoutBusSuffix(routeLongName);
    
    if (this.mode === RouteType.ReplacementBus) {
      result.route_id += '_BUS';
    } 
    return result;
  }

  private getNameAndColourWithoutBusSuffix(routeLongName : string) : {route_id : RouteID, name : string, long_name? : string, colour : number | null} {
    const rsid = this.rsid?.substring(0, 6) ?? this.tuid;
    const prefix = this.operator;

    // apply London Northwestern Railway and West Midlands Railway branding to West Midlands Trains services
    if (prefix === 'LM') {
      return [
        'Euston',
        'Watford Junction',
        'Tring',
        'Bletchley',
        'Milton Keynes',
        'Northampton',
        'St Albans',
        'Bedford',
        'Liverpool',
        'Crewe'
      ].find((element) => routeLongName.includes(element)) !== undefined
          ? {route_id : 'LN', name : 'LNR', long_name : 'London Northwestern Railway', colour : 0x00bf6f}
          : {route_id : 'WM', name : 'WMR', long_name : 'West Midlands Railway', colour : 0xe07709};
    }

    // apply Stansted Express branding
    if (prefix === 'LE' && ['London', 'Stansted Airport'].every((element) => routeLongName.includes(element))) {
      return {route_id : 'SX', name : 'Stansted Express', colour : 0x6b717a};
    }

    // identify the London Overground lines
    const callback = this.stopAtStation.bind(this);
    if (prefix === 'LO') {
      if (['SDC', 'ZCW', 'SQE', 'NXG', 'NWX', 'SYD', 'WCY', 'CYP'].some(callback)) {
        return {route_id : 'LO_Windrush', name : 'Windrush line', colour : 0xEF4D5E};
      }
      if (['LST', 'HAC', 'SKW', 'EDR', 'ENF', 'CHN', 'WST', 'CHI'].some(callback)) {
        return {route_id : 'LO_Weaver', name : 'Weaver line', colour : 0x972861};
      }
      if (['RMF', 'UPM'].some(callback)) {
        return {route_id : 'LO_Liberty', name : 'Liberty line', colour : 0x676767};
      }
      if (['KPA', 'SPB', 'RMD', 'SAT', 'HDH', 'CMD', 'HKC', 'SRA'].some(callback)) {
        return {route_id : 'LO_Mildmay', name : 'Mildmay line', colour : 0x437EC1};
      }
      // I am considering Stratford - Willesden / Watford through-running services to be Mildmay line here
      if (['EUS', 'KBN', 'SBP', 'HRW', 'WFH', 'WFJ'].some(callback)) {
        return {route_id : 'LO_Lioness', name : 'Lioness line', colour : 0xF1B41C};
      }
      if (['HRY', 'WMW', 'LER', 'BKG'].some(callback)) {
        return {route_id : 'LO_Suffragette', name : 'Suffragette line', colour : 0x39B97A};
      }
    }

    // identify the Merseyrail lines
    if (prefix === 'ME') {
      if (['HNX', 'LPY', 'SDL', 'BAH', 'HLR', 'SOP', 'KKD', 'WAO', 'MAG', 'OMS', 'RIL', 'KIR', 'HBL'].some(callback)) {
        return {route_id : 'ME_Northern', name : 'Northern line', colour : 0x0266b2};
      }
      if (['BKQ', 'NBN', 'BID', 'WKI', 'RFY', 'PSL', 'HOO', 'ELP', 'CTR'].some(callback)) {
        return {route_id : 'ME_Wirral', name : 'Wirral line', colour : 0x00a94f};
      }
    }

    // colours sourced from https://en.wikipedia.org/wiki/Wikipedia:WikiProject_UK_Railways/Colours_list
    const tocData = {
      "AW": {name: 'TfW Rail', long_name: "Transport for Wales", colour: 0xff0000},
      "CC": {name: "c2c", colour: 0xb7007c},
      "CH": {name: "Chiltern Railways", colour: 0x00bfff},
      "XC": {name: "CrossCountry", colour: 0x660f21},
      "GR": {name: "LNER", long_name: "London North Eastern Railway", colour: 0xce0e2d},
      "EM": {name: "EMR", long_name: "East Midlands Railway", colour: 0x713563},
      "ES": {name: "Eurostar", colour: 0xffd700},
      "GW": {name: "GWR", long_name: "Great Western Railway", colour: 0x0a493e},
      "HT": {name: "Hull Trains", colour: 0xde005c},
      "TP": {name: "TPE", long_name: "TransPennine Express", colour: 0x09a4ec },
      "GX": {name: "Gatwick Express", colour: 0xeb1e2d},
      "GC": {name: "Grand Central", colour: 0x1d1d1b},
      "GN": {name: "Great Northern", colour: 0x0099ff},
      "LE": {name: "Greater Anglia", colour: 0xd70428},
      "HX": {name: "Heathrow Express", colour: 0x532e63},
      "IL": {name: "Island Line", colour: 0x1e90ff},
      "LD": {name: "Lumo", colour: 0x2b6ef5},
      "LM": {name: 'WMT', long_name: "West Midlands Trains", colour: null},
      "LO": {name: "Overground", long_name: "London Overground", colour: 0xff7518},
      "LT": {name: "Underground", long_name: "London Underground", colour: 0x000f9f},
      "ME": {name: "Merseyrail", colour: 0xfff200},
      "NT": {name: "Northern", colour: 0x0f0d78},
      "SR": {name: "ScotRail", colour: 0x1e467d},
      "SW": {name: "SWR", long_name: "South Western Railway", colour: 0x24398c},
      "SE": {name: "Southeastern", colour: 0x389cff},
      "SN": {name: "Southern", colour: 0x8cc63e},
      "TL": {name: "Thameslink", colour: 0xff5aa4},
      "VT": {name: 'Avanti', long_name: "Avanti West Coast", colour: 0x004354},
      "TW": {name: "Metro", long_name: "Tyne & Wear Metro", colour: null},
      "CS": {name: "Caledonian Sleeper", colour: 0x1d2e35},
      "XR": {name: "Elizabeth line", colour: 0x9364cc},
      "QC": {name: "Caledonian MacBrayne", colour: null},
      "QS": {name: "Stena Line", colour: null},
      "ZZ": {name: "Other operator", colour: null}
    };

    return {route_id : prefix, ...tocData[prefix ?? ''] ?? {name : rsid, colour : null}};
  }

  /**
   * Convert to GTFS Route
   */
  public async toRoute(cifRepository : CIFRepository): Promise<Route> {
    const origin = await cifRepository.getStopName(this.origin) ?? this.origin;
    const destination = await cifRepository.getStopName(this.destination) ?? this.destination;
    const nameAndColour = this.getNameAndColour(`${origin} → ${destination}`);
    const agency_id = `=${this.operator ?? "ZZ"}`;
    return {
      route_id: nameAndColour.route_id,
      agency_id: agencies.some((a) => a.agency_id === agency_id) ? agency_id : '=ZZ',
      route_short_name: nameAndColour.name,
      route_long_name: nameAndColour.long_name ?? null,
      route_type: this.mode,
      route_text_color: null,
      route_color: nameAndColour.colour?.toString(16).padStart(6, '0') ?? null,
      route_url: null,
      route_desc: null,
    };
  }

  private get modeDescription(): string {
    switch (this.mode) {
      case RouteType.Rail: return "Train";
      case RouteType.Subway: return "Underground";
      case RouteType.Tram: return "Tram";
      case RouteType.Bus: return "Bus";
      case RouteType.ReplacementBus: return "Replacement bus";
      case RouteType.Ferry: return "Boat";
      default: return "Train";
    }
  }

  private get classDescription(): string {
    return this.firstClassAvailable ? "First class available" : "Standard class only";
  }

  private get reservationDescription(): string {
    return this.reservationPossible ? "Reservation possible" : "Reservation not possible";
  }

  public before(location: TIPLOC): StopTime[] {
    return this.stopTimes.slice(0, this.stopTimes.findIndex(s => s.tiploc_code === location));
  }

  public after(location: TIPLOC): StopTime[] {
    return this.stopTimes.slice(this.stopTimes.findIndex(s => s.tiploc_code === location) + 1);
  }

  public stopAt(location: TIPLOC): StopTime | undefined {
    return <StopTime>this.stopTimes.find(s => s.tiploc_code === location);
  }
  
  public stopAtStation(station: CRS): StopTime | undefined {
    return <StopTime>this.stopTimes.find(s => s.stop_code === station);
  }
}