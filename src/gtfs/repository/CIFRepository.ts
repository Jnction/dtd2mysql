
import {Pool} from 'mysql2';
import * as proj4 from 'proj4';
import {DatabaseConnection} from "../../database/DatabaseConnection";
import {Transfer} from "../file/Transfer";
import {AtcoCode, CRS, Stop, TIPLOC} from "../file/Stop";
import moment = require("moment");
import {ScheduleCalendar} from "../native/ScheduleCalendar";
import {Association, AssociationType, DateIndicator} from "../native/Association";
import {RSID, STP, TUID} from "../native/OverlayRecord";
import {ScheduleBuilder, ScheduleResults} from "./ScheduleBuilder";
import {Duration} from "../native/Duration";
import {FixedLink} from "../file/FixedLink";

/**
 * Provide access to the CIF/TTIS data in a vaguely GTFS-ish shape.
 */
export class CIFRepository {
  static readonly DATE_OFFSET_START = -7;
  static readonly DATE_OFFSET_END = 91;

  constructor(
    private readonly db: DatabaseConnection,
    private readonly stream: Pool,
    public stationCoordinates: StationCoordinates,
    public tiplocCoordinates: TiplocCoordiates = {},
    private additionalStops: Stop[] = [],
  ) {
    proj4.defs('EPSG:27700', '+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +ellps=airy +datum=OSGB36 +units=m +no_defs');
  }

  /**
   * Return the interchange time between each station
   */
  public async getTransfers(): Promise<Transfer[]> {
    const [results] = await this.db.query<Transfer>(`
      SELECT
        CONCAT('910G', tiploc_code) AS from_stop_id,
        CONCAT('910G', tiploc_code) AS to_stop_id,
        2 AS transfer_type, 
        minimum_change_time * 60 AS min_transfer_time 
      FROM physical_station WHERE cate_interchange_status <> 9
      GROUP BY crs_code
    `);

    return results;
  }

  /**
   * Return all the stops with some configurable long/lat applied
   */
  public async getStops(): Promise<Stop[]> {
    return [...(await this.stops).values()];
  }

  public async findStopById(stopId: string) {
    return (await this.stops).get(stopId);
  }

  /*
  Every passenger station in the National Rail network has a CRS code, however, some multi-part stations
  may have additional minor CRS code specifying a part of it. For example,
  STP (London St Pancras) has a minor code SPL representing the Thameslink platforms, while the main code represents the terminal platforms;
  PAD (London Paddington) has a minor code PDX representing the Crossrail platforms, while the main code represents the terminal platforms.

  In the database, such stations will have multiple entries, one for each TIPLOC code, where the one with cate_interchange_status <> 9 is the main entry
  which the CRS code (crs_code) and the minor CRS code (crs_reference_code) are the same.

  Using St Pancras as an example, there are 4 entries listed in the station database:
  TIPLOC    CRS    minor CRS    main entry?   MCT       location
  ------------------------------------------------------------------------------------
  STPX      STP    STP          *             15        Midland Main Line platforms
  STPADOM   STP    STP                        15        Domestic High Speed platforms
  STPXBOX   STP    SPL                        15        Thameslink platforms
  STPANCI   SPX    SPX          *             35        International platforms

  In the National Rail systems, the international station is treated as a distinct station from the domestic one,
  however the 3 remaining parts (Midland, Thameslink and High Speed Domestic) are the same station.

  The stop list will return one entry for each station (not its constituent parts) as a GTFS station identified by its main CRS code,
  and one entry for each TIPLOC (for unknown platform number) and each platform as a GTFS stop
  identified by its minor CRS code and the platform number, associated to the station with the main CRS code.
   */
  private stops : Promise<Map<string, Stop>> = (async () => {
    const [db_results] = await this.db.query<
      Omit<Stop, 'stop_lat' | 'stop_lon'> & {easting : number, northing : number, tiploc_code : string}
    >(`
      SELECT -- select all the physical stations
        CONCAT('910G', tiploc_code) AS stop_id, -- using the ATCO code as the stop ID
        crs_code AS stop_code, -- and the main CRS code as the public facing code
        station_name AS stop_name,
        NULL AS stop_desc,
        NULL AS zone_id,
        NULL AS stop_url,
        1 AS location_type,
        NULL AS parent_station,
        IF(POSITION('(CIE' IN station_name), 'Europe/Dublin', 'Europe/London') AS stop_timezone,
        0 AS wheelchair_boarding,
        NULL AS platform_code,
        easting,
        northing,
        tiploc_code
      FROM physical_station WHERE crs_code IS NOT NULL AND cate_interchange_status <> 9 -- from the main part of the station
      UNION SELECT -- and select all the platforms where scheduled services call at
        CONCAT('9100', physical_station.tiploc_code, IFNULL(platform, '')) AS stop_id, -- using the ATCO code with the platform number as the stop ID
        crs_reference_code AS stop_code, -- and the minor CRS code as the public facing code
        IF(ISNULL(platform), station_name, CONCAT(station_name, ' (Platform ', platform, ')')) AS stop_name,
        NULL AS stop_desc,
        NULL AS zone_id,
        NULL AS stop_url,
        0 AS location_type,
        (select CONCAT('910G', tiploc_code) from physical_station parent where crs_code = physical_station.crs_code and cate_interchange_status <> 9) AS parent_station,
        IF(POSITION('(CIE' IN station_name), 'Europe/Dublin', 'Europe/London') AS stop_timezone,
        0 AS wheelchair_boarding,
        platform AS platform_code,
        easting,
        northing,
        physical_station.tiploc_code
      FROM physical_station
        INNER JOIN (
          SELECT DISTINCT
            location AS tiploc_code, 
            cast(NULL AS CHAR(3)) COLLATE utf8mb4_unicode_ci AS crs_code, 
            platform 
          FROM stop_time
          UNION SELECT DISTINCT 
            tiploc_code,
            cast(NULL AS CHAR(3)) COLLATE utf8mb4_unicode_ci AS crs_code,
            NULL as platform
          FROM physical_station WHERE crs_code IS NOT NULL
        ) platforms ON physical_station.tiploc_code = platforms.tiploc_code OR (physical_station.crs_code = platforms.crs_code AND physical_station.cate_interchange_status <> 9)
      -- ANSL3 is a tiploc code representing the track leading to platform 3 of Anniesland
      -- The ATCO generation will then clash with the platform 3 at the TIPLOC ANSL, which represents the whole of Anniesland station
      -- The schedule data uses ANSL for trains starting from platform 3, refer to ScotRail Mo-Fr 18:03 service from Anniesland to Glasgow Queen Street for details
      WHERE physical_station.crs_code IS NOT NULL AND physical_station.tiploc_code <> 'ANSL3'
    `);
    const results : Stop[] = [...this.additionalStops, ...db_results.map(row => {
      const [stop_lon, stop_lat] = proj4('EPSG:27700', 'EPSG:4326', [(row.easting - 10000) * 100, (row.northing - 60000) * 100]);
      const {easting, northing, ...stop} = {...row, stop_lon, stop_lat};
      return stop;
    })];

    const stopById = new Map<string, Stop>();

    // overlay the long and latitude values from configuration
    for (const stop of results.map(stop => {
      const tiploc_entry = this.tiplocCoordinates[stop.tiploc_code];
      const station_data =
          this.stationCoordinates[stop.stop_code]
          ?? this.stationCoordinates[results.find(parent_stop => parent_stop.stop_id === stop.parent_station)?.stop_code ?? '']
          ?? tiploc_entry
      if (stop.location_type === 0) {
        const platform_code = stop.platform_code;
        if (platform_code) {
          const platform_data = (station_data?.platforms ?? [])[platform_code];
          if (platform_data !== undefined) {
            // use platform data if available
            return Object.assign(stop, platform_data);
          }
        }
        if (station_data !== undefined) {
          // otherwise inherit station data
          const result = Object.assign(stop, station_data);
          delete result['platforms'];
          result.stop_name += platform_code ? ` (Platform ${platform_code})` : '';
          result.location_type = 0;
          // for platformless stops, try to calculate an average of platforms first, failing that, 
          // use tiploc coordinates instead of station coordinates
          const platforms = Object.entries(station_data.platforms ?? []).filter(
              entry => results.find(result => result.stop_id === `9100${stop.tiploc_code}${entry[0]}`)
          ).map(entry => entry[1]);
          if (platforms.length === 0) {
            if (tiploc_entry !== undefined) {
              result.stop_lat = tiploc_entry.stop_lat;
              result.stop_lon = tiploc_entry.stop_lon;
            }
          } else {
            result.stop_lat = platforms.reduce((sum, platform) => sum + platform.stop_lat, 0) / platforms.length;
            result.stop_lon = platforms.reduce((sum, platform) => sum + platform.stop_lon, 0) / platforms.length;
          }
          return result;
        } else {
          return stop;
        }
      } else {
        const result = Object.assign(stop, station_data);
        delete result['platforms'];
        return result;
      }
    })) {
      stopById.set(stop.stop_id, stop);
    }
    
    return stopById;
  })();

  /**
   * Return the schedules and z trains. These queries probably require some explanation:
   *
   * The first query selects the stop times for all passenger services between now and + 3 months. It's important that
   * the stop time location is mapped to physical stations to avoid getting fake CRS codes from the tiploc data.
   *
   * The second query selects all the z-trains (usually replacement buses) within three months. They already use CRS
   * codes as the location so avoid the disaster above.
   */
  public async getSchedules(): Promise<ScheduleResults> {
    const scheduleBuilder = new ScheduleBuilder();
    const [[lastSchedule]] = await this.db.query<{id: number}>("SELECT id FROM schedule ORDER BY id desc LIMIT 1");

    await Promise.all([
      scheduleBuilder.loadSchedules(this.stream.query(`
        SELECT
          schedule.id AS id, train_uid, retail_train_id, runs_from, runs_to,
          monday, tuesday, wednesday, thursday, friday, saturday, sunday,
          CONCAT('9100', location, IFNULL(platform, '')) as atco_code, location, crs_code, stp_indicator, public_arrival_time, public_departure_time,
          IF(train_status='S', 'SS', train_category) AS train_category,
          scheduled_arrival_time AS scheduled_arrival_time,
          scheduled_departure_time AS scheduled_departure_time,
          platform, atoc_code, stop_time.id AS stop_id, activity, reservations, train_class
        FROM schedule
        LEFT JOIN schedule_extra ON schedule.id = schedule_extra.schedule
        LEFT JOIN (stop_time LEFT JOIN physical_station ps ON location = ps.tiploc_code) ON schedule.id = stop_time.schedule
        WHERE runs_from < CURDATE() + INTERVAL ${CIFRepository.DATE_OFFSET_END} DAY
        AND runs_to >= CURDATE() + INTERVAL ${CIFRepository.DATE_OFFSET_START} DAY
        AND (IF(train_status='S', 'SS', ifnull(train_category, '')) NOT IN ('OL', 'SS', 'BS'))
        AND ifnull(atoc_code, '') NOT IN ('LT', 'TW', 'ES')
        ORDER BY stp_indicator DESC, id, stop_id
      `)),
    ]);

    return scheduleBuilder.results;
  }

  /**
   * Get associations
   */
  public async getAssociations(): Promise<Association[]> {
    const [results] = await this.db.query<AssociationRow>(`
      SELECT 
        a.id AS id, base_uid, assoc_uid, assoc_location, assoc_date_ind, assoc_cat,
        monday, tuesday, wednesday, thursday, friday, saturday, sunday,
        start_date, end_date, stp_indicator
      FROM association a
      WHERE start_date < CURDATE() + INTERVAL ${CIFRepository.DATE_OFFSET_END} DAY
      AND end_date >= CURDATE() + INTERVAL ${CIFRepository.DATE_OFFSET_START} DAY
      ORDER BY stp_indicator DESC, id
    `);

    return results.map(row => new Association(
      row.id,
      row.base_uid,
      row.assoc_uid,
      row.assoc_location,
      row.assoc_date_ind,
      row.assoc_cat,
      new ScheduleCalendar(
        moment(row.start_date),
        moment(row.end_date), {
        0: row.sunday,
        1: row.monday,
        2: row.tuesday,
        3: row.wednesday,
        4: row.thursday,
        5: row.friday,
        6: row.saturday
      }),
      row.stp_indicator
    ));
  }

  /**
   * Return the ALF information
   */
  public async getFixedLinks(): Promise<FixedLink[]> {
    // use the additional fixed links if possible and fill the missing data with fixed_links
    const [rows] = await this.db.query<FixedLinkRow>(`
      SELECT
        mode, duration * 60 as duration, origin, destination,
        start_time, end_time, start_date, end_date,
        monday, tuesday, wednesday, thursday, friday, saturday, sunday
      FROM additional_fixed_link
      WHERE origin IN (SELECT crs_code FROM physical_station)
      AND destination IN (SELECT crs_code FROM physical_station)
      UNION
      SELECT
        mode, duration * 60 as duration, origin, destination,
        "00:00:00", "23:59:59", "2017-01-01", "2038-01-19",
        1,1,1,1,1,1,1
      FROM fixed_link
      WHERE CONCAT(origin, destination) NOT IN (
        SELECT CONCAT(origin, destination) FROM additional_fixed_link
      )
    `);

    const results: FixedLink[] = [];

    for (const row of rows) {
      const origin = await this.getStopId(row.origin);
      const destination = await this.getStopId(row.destination);
      if (origin === null || destination === null) {
        throw new Error(`The stations of ${row.origin} or ${row.destination} cannot be found.`);
      }
      results.push(this.getFixedLinkRow(origin, destination, row));
      results.push(this.getFixedLinkRow(destination, origin, row));
    }

    return results;
  }

  private getFixedLinkRow(origin: AtcoCode, destination: AtcoCode, row: FixedLinkRow): FixedLink {
    return {
      from_stop_id: origin,
      to_stop_id: destination,
      mode: row.mode,
      duration: row.duration,
      start_time: row.start_time,
      end_time: row.end_time,
      start_date: (row.start_date || "2017-01-01"),
      end_date: (row.end_date || "2038-01-19"),
      monday: row.monday,
      tuesday: row.tuesday,
      wednesday: row.wednesday,
      thursday: row.thursday,
      friday: row.friday,
      saturday: row.saturday,
      sunday: row.sunday
    };
  }

  /**
   * Close the underlying database
   */
  public end(): Promise<any> {
    return Promise.all([this.db.end(), this.stream.end()]);
  }

  public async getStopName(stop_id : AtcoCode) : Promise<string | null> {
    const longName = await this.getFullStopName(stop_id);
    if (longName === null || longName.toUpperCase().includes('MAESTEG')) {
      return longName;
    }
    return longName.replace(/ \(.*\)$/g, '');
  }

  public async getStopId(code : CRS) : Promise<AtcoCode | null> {
    const stop_data = await this.getStops();
    const result = stop_data.find(
        (value) => value.stop_code === code && value.location_type === 1
    );
    return result === undefined ? null : result.stop_id;
  }

  public async getFullStopName(stop_id : AtcoCode) : Promise<string | null> {
    return (await this.findStopById(stop_id))?.stop_name ?? null;
  }
}

export interface ScheduleStopTimeRow {
  id: number,
  train_uid: TUID,
  retail_train_id: RSID,
  runs_from: string,
  runs_to: string,
  monday: 0 | 1,
  tuesday: 0 | 1,
  wednesday: 0 | 1,
  thursday: 0 | 1,
  friday: 0 | 1,
  saturday: 0 | 1,
  sunday: 0 | 1,
  stp_indicator: STP,
  atco_code: AtcoCode,
  location: TIPLOC,
  crs_code: CRS,
  train_category: string,
  atoc_code: string | null,
  public_arrival_time: string | null,
  public_departure_time: string | null,
  scheduled_arrival_time: string | null,
  scheduled_departure_time: string | null,
  platform: string,
  activity: string | null,
  train_class: null | "S" | "B",
  reservations: null | "R" | "S" | "A"
}

export type StationCoordinates = {
  [crs: CRS]: {
    stop_lat: number,
    stop_lon: number,
    stop_name: string,
    location_type?: number,
    wheelchair_boarding: 0 | 1 | 2,
    platforms?: {[key : string] : StationCoordinates[string]}
  }
};

export type TiplocCoordiates = {
  [tiploc: TIPLOC]: {
    stop_lat: number,
    stop_lon: number,
    stop_name: string,
    stop_desc: string,
  }
}

export type ViaText = {
  [crs: CRS]: {
    At: CRS,
    Dest: TIPLOC,
    Loc1: TIPLOC,
    Loc2: TIPLOC | null,
    Viatext: string
  }[]
};


interface AssociationRow {
  id: number;
  base_uid: string;
  assoc_uid: string;
  assoc_location: TIPLOC;
  start_date: string;
  end_date: string;
  assoc_date_ind: DateIndicator,
  assoc_cat: AssociationType,
  sunday: 0 | 1;
  monday: 0 | 1;
  tuesday: 0 | 1;
  wednesday: 0 | 1;
  thursday: 0 | 1;
  friday: 0 | 1;
  saturday: 0 | 1;
  stp_indicator: STP;
}

interface FixedLinkRow {
  mode: FixedLinkMode;
  duration: Duration;
  origin: CRS;
  destination: CRS;
  start_time: string;
  end_time: string;
  start_date: string | null;
  end_date: string | null;
  monday: 0 | 1;
  tuesday: 0 | 1;
  wednesday: 0 | 1;
  thursday: 0 | 1;
  friday: 0 | 1;
  saturday: 0 | 1;
  sunday: 0 | 1;
}

enum FixedLinkMode {
  Walk = "WALK",
  Metro = "METRO",
  Transfer = "TRANSFER",
  Tube = "TUBE",
  Bus = "BUS"
}
