import {AgencyID} from "../file/Agency";
import {Route, RouteType} from "../file/Route";
import {Shape} from '../file/Shape';
import {CRS} from "../file/Stop";
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
    public readonly stopTimes: StopTime[],
    public readonly tuid: TUID,
    public readonly rsid: RSID | null,
    public readonly calendar: ScheduleCalendar,
    public readonly mode: RouteType,
    public readonly operator: AgencyID | null,
    public readonly stp: STP,
    public readonly firstClassAvailable: boolean,
    public readonly reservationPossible: boolean
  ) {}

  public get origin(): CRS {
    return this.stopTimes[0].stop_id.substr(0, 3);
  }

  public get destination(): CRS {
    return this.stopTimes[this.stopTimes.length - 1].stop_id.substr(0, 3);
  }

  public get hash(): string {
    return this.tuid + this.stopTimes.map(s => s.stop_id + s.departure_time + s.arrival_time).join("") + this.calendar.binaryDays;
  }

  /**
   * Clone the current record with the new calendar and id
   */
  public clone(calendar: ScheduleCalendar, scheduleId: number): Schedule {
    return new Schedule(
      scheduleId,
      this.stopTimes.map(st => Object.assign({}, st, { trip_id: scheduleId })),
      this.tuid,
      this.rsid,
      calendar,
      this.mode,
      this.operator,
      this.stp,
      this.firstClassAvailable,
      this.reservationPossible
    );
  }

  /**
   * Convert to a GTFS Trip
   */
  public async toTrip(serviceId: string, routeId: number, cifRepository : CIFRepository): Promise<Trip> {
    return {
      route_id: routeId,
      service_id: serviceId,
      trip_id: this.id,
      trip_headsign: await cifRepository.getStopName(this.destination) ?? this.destination,
      trip_short_name: this.rsid?.substr(0, 6) ?? this.tuid,
      direction_id: 0,
      shape_id: this.id,
      wheelchair_accessible: 1,
      bikes_allowed: 0,
    };
  }

  /**
   * Convert to GTFS Shape Points
   */
  public async toShape(cifRepository : CIFRepository): Promise<Shape[]> {
    const result : Shape[] = [];
    let sequence = 0;
    for (const stopTime of this.stopTimes) {
      const stop = await cifRepository.findStopById(stopTime.stop_id);
      if (stop !== undefined && stop.stop_lat !== null && stop.stop_lon !== null) {
        result.push({
          shape_id: this.id,
          shape_pt_lat: stop.stop_lat,
          shape_pt_lon: stop.stop_lon,
          shape_pt_sequence: sequence++,
        })
      }
    }
    return result;
  }

  public getNameAndColour(routeLongName : string) : {name : string, colour : number | null} {
    const rsid = this.rsid?.substring(0, 6) ?? this.tuid;
    const prefix = this.operator ?? null;
    // colours sourced from https://en.wikipedia.org/wiki/Wikipedia:WikiProject_UK_Railways/Colours_list
    const tocData = {
      "AW": {name: "Transport for Wales", colour: 0xff0000},
      "CC": {name: "c2c", colour: 0xb7007c},
      "CH": {name: "Chiltern Railways", colour: 0x00bfff},
      "XC": {name: "CrossCountry", colour: 0x660f21},
      "GR": {name: "LNER", colour: 0xce0e2d},
      "EM": {name: "EMR", colour: 0x713563},
      "ES": {name: "Eurostar", colour: 0xffd700},
      "GW": {name: "GWR", colour: 0x0a493e},
      "HT": {name: "Hull Trains", colour: 0xde005c},
      "TP": {name: "TransPennine Express", colour: 0x09a4ec },
      "GX": {name: "Gatwick Express", colour: 0xeb1e2d},
      "GC": {name: "Grand Central", colour: 0x1d1d1b},
      "GN": {name: "Great Northern", colour: 0x0099ff},
      "LE": {name: "Greater Anglia", colour: 0xd70428},
      "HX": {name: "Heathrow Express", colour: 0x532e63},
      "IL": {name: "Island Line", colour: 0x1e90ff},
      "LD": {name: "Lumo", colour: 0x2b6ef5},
      "LM": {name: "West Midlands Trains", colour: null},
      "LO": {name: "London Overground", colour: 0xff7518},
      "LT": {name: "London Underground", colour: 0x000f9f},
      "ME": {name: "Merseyrail", colour: 0xfff200},
      "NT": {name: "Northern", colour: 0x0f0d78},
      "SR": {name: "ScotRail", colour: 0x1e467d},
      "SW": {name: "South Western Railway", colour: 0x24398c},
      "SE": {name: "Southeastern", colour: 0x389cff},
      "SN": {name: "Southern", colour: 0x8cc63e},
      "TL": {name: "Thameslink", colour: 0xff5aa4},
      "VT": {name: "Avanti West Coast", colour: 0x004354},
      "TW": {name: "Nexus (Tyne & Wear Metro)", colour: null},
      "CS": {name: "Caledonian Sleeper", colour: 0x1d2e35},
      "XR": {name: "Elizabeth line", colour: 0x9364cc},
      "QC": {name: "Caledonian MacBrayne", colour: null},
      "QS": {name: "Stena Line", colour: null},
      "ZZ": {name: "Other operator", colour: null}
    };

    if (prefix === 'LM') {
      return [
        'Euston',
        'Watford Junction',
        'Tring',
        'Bletchley',
        'Milton Keynes',
        'St Albans',
        'Bedford',
        'Liverpool',
        'Crewe'
      ].find((element) => routeLongName.includes(element)) !== undefined
          ? {name : 'London Northwestern Railway', colour : 0x00bf6f}
          : {name : 'West Midlands Railway', colour : 0xe07709};
    }
    if (prefix === 'LE') {
      return ['London', 'Stansted Airport'].every((element) => routeLongName.includes(element))
          ? {name : 'Stansted Express', colour : 0x6b717a}
          : tocData.LE;
    }
    // fixme: should I handle line names of Merseyrail
    return tocData[prefix ?? ''] ?? {name: rsid, colour : null};
  }


  /**
   * Convert to GTFS Route
   */
  public async toRoute(cifRepository : CIFRepository): Promise<Route> {
    const origin = await cifRepository.getStopName(this.origin) ?? this.origin;
    const destination = await cifRepository.getStopName(this.destination) ?? this.destination;
    const nameAndColour = this.getNameAndColour(`${origin} → ${destination}`);
    return {
      route_id: this.id,
      agency_id: this.operator || "ZZ",
      route_short_name: nameAndColour.name,
      route_long_name: null,
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

  public before(location: CRS): StopTime[] {
    return this.stopTimes.slice(0, this.stopTimes.findIndex(s => s.stop_id.substr(0, 3) === location));
  }

  public after(location: CRS): StopTime[] {
    return this.stopTimes.slice(this.stopTimes.findIndex(s => s.stop_id.substr(0, 3) === location) + 1);
  }

  public stopAt(location: CRS): StopTime | undefined {
    return <StopTime>this.stopTimes.find(s => s.stop_id.substr(0, 3) === location);
  }
}

