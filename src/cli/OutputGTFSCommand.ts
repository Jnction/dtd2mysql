import * as moment from 'moment';
import {Attribution} from '../gtfs/file/Attribution';
import {FeedInfo} from '../gtfs/file/FeedInfo';
import {Route, RouteType} from '../gtfs/file/Route';
import {CLICommand} from "./CLICommand";
import {CIFRepository} from "../gtfs/repository/CIFRepository";
import {Schedule} from "../gtfs/native/Schedule";
import {agencies} from "../../config/gtfs/agency";
import {Association} from "../gtfs/native/Association";
import {applyOverlays} from "../gtfs/command/ApplyOverlays";
import {mergeSchedules} from "../gtfs/command/MergeSchedules";
import {applyAssociations, AssociationIndex, ScheduleIndex} from "../gtfs/command/ApplyAssociations";
import {createCalendar, ServiceIdIndex} from "../gtfs/command/CreateCalendar";
import {ScheduleBuilder, ScheduleResults} from "../gtfs/repository/ScheduleBuilder";
import {GTFSOutput} from "../gtfs/output/GTFSOutput";
import * as fs from "fs";
import {addLateNightServices} from "../gtfs/command/AddLateNightServices";
import {Accessibility} from "../gtfs/file/Trip";
import streamToPromise = require("stream-to-promise");
import objectHash = require('object-hash');

export class OutputGTFSCommand implements CLICommand {
  private baseDir: string;

  public constructor(
    private readonly repository: CIFRepository,
    private readonly output: GTFSOutput
  ) {}

  /**
   * Turn the timetable feed into GTFS files
   */
  public async run(argv: string[]): Promise<void> {
    this.baseDir = argv[3] || ".";

    if (!fs.existsSync(this.baseDir)) {
      throw new Error(`Output path ${this.baseDir} does not exist.`);
    }
    
    let disableStationMct = false;
    if (argv.find(arg => arg === "--disable-station-mct")) {
        disableStationMct = true;
        // noinspection AssignmentToFunctionParameterJS
        argv = argv.filter(arg => arg !== "--disable-station-mct");
    }

    if (argv.length > 4) {
      const json = JSON.parse(fs.readFileSync(argv[4], 'utf-8'));
      this.repository.stationCoordinates = json;
    }

    const associationsP = this.repository.getAssociations();
    const scheduleResultsP = this.repository.getSchedules();
    const now = new Date();
    const infoP = this.copy([<FeedInfo>{
      feed_publisher_name: 'Aubin',
      feed_publisher_url: 'https://aubin.app',
      feed_lang: 'en',
      feed_start_date: moment(new Date(now.setDate(now.getDate() + CIFRepository.DATE_OFFSET_START))).format('YYYYMMDD'),
      feed_end_date: moment(new Date(now.setDate(now.getDate() + CIFRepository.DATE_OFFSET_END))).format('YYYYMMDD'),
    }], "feed_info.txt");
    const attributionP = this.copy(<Attribution[]>[
      {
        organization_name : "National Rail Enquiries",
        is_producer : 0,
        is_operator : 0,
        is_authority : 1,
        attribution_url : "https://opendata.nationalrail.co.uk/terms",
      },
      {
        organization_name : "Aubin MaaS Limited",
        is_producer : 1,
        is_operator : 0,
        is_authority : 0,
      },
    ], "attributions.txt");
    const transfersP = this.copy(this.repository.getTransfers(disableStationMct), "transfers.txt");
    const stopsP = this.copy(this.repository.getStops(), "stops.txt");
    const agencyP = this.copy(agencies, "agency.txt");
    const fixedLinksP = this.copy(this.repository.getFixedLinks(), "links.txt");

    const schedules = await this.getSchedules(await associationsP, await scheduleResultsP);
    const [calendars, calendarDates, serviceIds] = createCalendar(schedules);

    const calendarP = this.copy(calendars, "calendar.txt");
    const calendarDatesP = this.copy(calendarDates, "calendar_dates.txt");
    const tripsP = this.copyTrips(schedules, serviceIds);

    await Promise.all([
      infoP,
      attributionP,
      agencyP,
      transfersP,
      stopsP,
      calendarP,
      calendarDatesP,
      tripsP,
      fixedLinksP,
      this.repository.end(),
      this.output.end()
    ]);
  }

  /**
   * Map SQL records to a file
   */
  private async copy(results: object[] | Promise<object[]>, filename: string): Promise<void> {
    const rows = await results;
    const output = this.output.open(`${this.baseDir}/${filename}`);

    console.log("Writing " + filename);
    rows.forEach(row => output.write(row));
    output.end();

    return streamToPromise(output);
  }

  /**
   * trips.txt, stop_times.txt and routes.txt have interdependencies so they are written together
   */
  private async copyTrips(schedules: Schedule[], serviceIds: ServiceIdIndex): Promise<any> {
    console.log("Writing trips.txt, stop_times.txt, routes.txt and shapes.txt");
      const trips = this.output.open(`${this.baseDir}/trips.txt`);
      const stopTimes = this.output.open(`${this.baseDir}/stop_times.txt`);
      const routeFile = this.output.open(`${this.baseDir}/routes.txt`);
    const shapes = this.output.open(`${this.baseDir}/shapes.txt`);
    const routes : {[key : string] : Route} = {};
    const writtenShapes = new Set();

    function getRouteHash(route : Route) {
      return objectHash(`${route.agency_id}_${route.route_type}_${route.route_short_name}_${route.route_long_name}_${route.route_color}_${route.route_text_color}`);
    }

    for (const schedule of schedules) {
      if (schedule.stopTimes.length <= 1) {
        continue;
      }

      const route = await schedule.toRoute(this.repository);
      // group schedules with the same hash into the same GTFS route
      const routeHash = getRouteHash(route);
      routes[routeHash] = routes[routeHash] || route;
      const routeId = routes[routeHash].route_id;
      const serviceId = serviceIds[schedule.calendar.id];
      const bikesAllowed = (() => {
        // TODO: need a way to define temporary bike ban

        if (route.route_type === RouteType.ReplacementBus) {
          return Accessibility.NO;
        }

        const operator = schedule.operator;
        // Lumo trains don't allow bikes at all
        if (operator === 'LD' || operator === 'LF') {
          return Accessibility.NO;
        }

        // Stansted Express trains don't allow bike at all
        if (route.route_short_name === 'Stansted Express') {
          return Accessibility.NO;
        }

        // The following operators have some peak restrictions. Leave them as unknown if the train runs on a weekday
        // before GTFS gets support for stop-specific restrictions.
        // TODO: handle bank holidays
        // https://github.com/google/transit/issues/466
        if ([1, 2, 3, 4, 5].some(weekday => schedule.calendar.days[weekday])
            && operator !== null) {
            function arrivesBetween(crs: string, start: string, end: string) {
                const stop = schedule.stopAtStation(crs);
                return stop !== undefined && stop !== schedule.stopTimes[0] && stop.arrival_time !== null && stop.arrival_time > start && stop.arrival_time <= end;
            }
            
            function departsBetween(crs: string, start: string, end: string) {
                const stop = schedule.stopAtStation(crs);
                return stop !== undefined && stop !== schedule.stopTimes[schedule.stopTimes.length - 1] && stop.departure_time !== null && stop.departure_time >= start && stop.departure_time < end;
            }
            
            switch (operator) {
            case 'XR': {
                for (const stop of ['PAD', 'PDX', 'LST', 'LSX']) {
                    if (arrivesBetween(stop, "07:30:00", "09:30:00")) {
                        // no bikes into Central London, may be allowed out of it
                        return Accessibility.UNKNOWN;
                    }
                    if (departsBetween(stop, "16:00:00", "19:00:00")) {
                        // no bikes out of Central London, may be allowed into it
                        return Accessibility.UNKNOWN;
                    }
                }
                // between Paddington, Liverpool Street or Abbey Wood
                {
                    // eastbound
                    const paddington = schedule.stopAtStation('PDX')?.departure_time;
                    const liverpool_street = schedule.stopAtStation('LSX')?.arrival_time;
                    const abbey_wood = schedule.stopAtStation('ABX')?.arrival_time;
                    if (paddington != null && liverpool_street != null && liverpool_street > paddington) {
                        if (paddington >= "07:30:00" && paddington < "09:30:00" || paddington >= "16:00:00" && paddington < "19:00:00") {
                            return Accessibility.UNKNOWN;
                        }
                        for (const stop of [liverpool_street, abbey_wood]) {
                            if (stop != null && (
                                stop > "07:30:00" && stop <= "09:30:00" || stop > "16:00:00" && stop <= "19:00:00"
                            )) {
                                return Accessibility.UNKNOWN;
                            }
                        }
                    }
                }
                {
                    // westbound
                    const paddington = schedule.stopAtStation('PDX')?.arrival_time;
                    const liverpool_street = schedule.stopAtStation('LSX')?.departure_time;
                    const abbey_wood = schedule.stopAtStation('ABX')?.departure_time;
                    if (paddington != null && liverpool_street != null && liverpool_street < paddington) {
                        if (paddington > "07:30:00" && paddington <= "09:30:00" || paddington > "16:00:00" && paddington <= "19:00:00") {
                            return Accessibility.UNKNOWN;
                        }
                        for (const stop of [liverpool_street, abbey_wood]) {
                            if (stop != null && (
                                stop >= "07:30:00" && stop < "09:30:00" || stop >= "16:00:00" && stop < "19:00:00"
                            )) {
                                return Accessibility.UNKNOWN;
                            }
                        }
                    }
                }
                break;
            }
            case 'LO': {
                if (schedule.stopTimes[0].stop_code === 'LST') {
                    if (departsBetween('LST', "16:00:00", "19:00:00")) {
                        return Accessibility.NO;
                    }
                } else if (schedule.stopTimes[schedule.stopTimes.length - 1].stop_code === 'LST') {
                    if (arrivesBetween('LST', "07:30:00", "09:30:00")) {
                        return Accessibility.NO;
                    }
                } else {
                    for (let i = 0; i < schedule.stopTimes.length - 1; i++) {
                        const departure_time = schedule.stopTimes[i].departure_time;
                        const arrival_time = schedule.stopTimes[i + 1].arrival_time;
                        if (departure_time !== null && arrival_time !== null) {
                            if (departure_time >= "07:30:00" && departure_time < "09:30:00" || arrival_time > "07:30:00" && arrival_time <= "09:30:00") {
                                return Accessibility.UNKNOWN;
                            }
                            if (departure_time >= "16:00:00" && departure_time < "19:00:00" || arrival_time > "16:00:00" && arrival_time <= "19:00:00") {
                                return Accessibility.UNKNOWN;
                            }
                        }
                    }
                }
            }
                break;
            case "HX":
                if (schedule.stopTimes[0].stop_code === 'PAD') {
                    if (departsBetween('PAD', "16:30:00", "19:00:00")) {
                        return Accessibility.NO
                    }
                }
                if (schedule.stopTimes[schedule.stopTimes.length - 1].stop_code === 'PAD') {
                    if (departsBetween('HXX', "06:30:00", "10:00:00")) {
                        return Accessibility.NO;
                    }
                }
                break;
            case "TL": {
                for (const stop of ['STP', 'SPL', 'LBG', 'BFR']) {
                    if (arrivesBetween(stop, "07:00:00", "10:00:00")) {
                        return Accessibility.UNKNOWN;
                    }
                    if (departsBetween(stop, "16:00:00", "19:00:00")) {
                        return Accessibility.UNKNOWN;
                    }
                }
            }
                break;
            case "GN": {
                for (const stop of ['KGX', 'MOG']) {
                    if (arrivesBetween(stop, "07:00:00", "09:30:00")) {
                        // it is a no, but there is an exception between Stevenage and Hertford North
                        return Accessibility.UNKNOWN;
                    }
                    if (departsBetween(stop, "16:00:00", "19:00:00")) {
                        return Accessibility.UNKNOWN;
                    }
                }
                if (arrivesBetween('CBG', "07:45:00", "08:45:00")) {
                    const arrival_at_cambridge = schedule.stopAtStation('CBG')?.arrival_time!;
                    for (const stop of ['KLN', 'ELY', 'CMB']) {
                        const arrival = schedule.stopAtStation(stop)?.arrival_time;
                        if (arrival != null && arrival < arrival_at_cambridge) {
                            // No before Cambridge
                            return Accessibility.UNKNOWN;
                        }
                    }
                }
            }
                break;
            case "GX": {
                if (schedule.stopTimes[0].stop_code === 'VIC') {
                    const departure_time = schedule.stopTimes[0].departure_time;
                    if (departure_time !== null && departure_time >= "16:00:00" && departure_time < "19:00:00") {
                        return Accessibility.NO;
                    }
                }
                if (schedule.stopTimes[schedule.stopTimes.length - 1].stop_code === 'VIC') {
                    const arrival_time = schedule.stopTimes[schedule.stopTimes.length - 1].arrival_time;
                    if (arrival_time !== null && arrival_time > "07:00:00" && arrival_time <= "10:00:00") {
                        return Accessibility.NO;
                    }
                }
            }
                break;
            case "SN": {
                const victoria = schedule.stopAtStation('VIC');
                const london_bridge = schedule.stopAtStation('LBG');
                const kensington_olympia = schedule.stopAtStation('KPA');
                const brighton = schedule.stopAtStation('BTN');
                for (const stop of [victoria, london_bridge, kensington_olympia, brighton]) {
                    if (stop !== schedule.stopTimes[0] && stop?.arrival_time != null && stop.arrival_time > "07:00:00" && stop.arrival_time <= "10:00:00") {
                        // it is a no, but there may be exceptions on part of the route
                        return Accessibility.UNKNOWN;
                    }
                    if (stop !== schedule.stopTimes[schedule.stopTimes.length - 1] && stop?.departure_time != null && stop.departure_time > "16:00:00" && stop.departure_time <= "19:00:00") {
                        return Accessibility.UNKNOWN;
                    }
                }
            }
                break;
            case "CC":
                if (arrivesBetween('FST', "07:14:00", "09:30:00") || departsBetween('FST', "16:30:00", "18:35:00")) {
                    return Accessibility.NO;
                }
                break;
            case 'CH':
                for (const stop of ['MYB', 'OXF', 'BMO']) {
                    if (arrivesBetween(stop, "07:45:00", "10:00:00") || departsBetween(stop, "16:30:00", "19:30:00")) {
                        return Accessibility.NO;
                    }
                }
                break;
            case 'GW':
                if (arrivesBetween('PAD', "07:30:00", "09:30:00") || departsBetween('PAD', "16:00:00", "19:00:00")) {
                    return Accessibility.NO;
                }
                break;
            case 'SE':
                for (const stop of ['STP', 'CHX', 'CST', 'VIC']) {
                    if (arrivesBetween(stop, "07:00:00", "10:00:00") || departsBetween(stop, "16:00:00", "19:00:00")) {
                        // no only within the commuter area
                        return Accessibility.UNKNOWN;
                    }
                }
                break;
            case 'SW':
                if (arrivesBetween('WAT', "07:15:00", "10:00:00") || departsBetween('WAT', "16:45:00", "19:00:00")) {
                    // restriction on intermediate stations
                    return Accessibility.UNKNOWN;
                }
                if (departsBetween('CLJ', "07:45:00", "09:00:00")) {
                    const departure_time = schedule.stopAtStation('CLJ')?.departure_time!;
                    const arrival_time = schedule.stopAtStation('RMD')?.arrival_time
                        ?? schedule.stopAtStation('HOU')?.arrival_time;
                    if (arrival_time != null && arrival_time > departure_time) {
                        // restriction on Hounslow loop
                        return Accessibility.UNKNOWN;
                    }
                }
                break;
            case 'EM':
                if (schedule.stopTimes[0].stop_code === 'COR' && arrivesBetween('STP', "04:30:00", "10:00:00")) {
                    return Accessibility.NO;
                }
                if (schedule.stopTimes[schedule.stopTimes.length - 1].stop_code === 'COR' && departsBetween(
                    'STP',
                    "16:00:00",
                    "19:00:00"
                )) {
                    return Accessibility.NO;
                }
                break;
            case 'LM':
                if (arrivesBetween('EUS', "07:00:00", "10:00:00") || departsBetween('EUS', "16:00:00", "19:00:00")) {
                    return Accessibility.NO;
                }
                break;
            case 'LE':
                if (arrivesBetween('CBG', "07:45:00", "08:45:00")) {
                    return Accessibility.NO;
                }
                if (arrivesBetween('LST', "07:45:00", "09:45:00") || departsBetween('LST', "16:30:00", "18:30:00")) {
                    if (schedule.stopTimes[0].stop_code !== 'NRW' && schedule.stopTimes[schedule.stopTimes.length - 1].stop_code !== 'NRW') {
                        return Accessibility.NO;
                    }
                }
                if (arrivesBetween('SRA', "07:45:00", "09:45:00") && schedule.stopTimes[0].stop_code !== 'LST' 
                    || departsBetween('SRA', "16:30:00", "18:30:00")  && schedule.stopTimes[schedule.stopTimes.length - 1].stop_code !== 'LST' ) {
                    if (schedule.stopTimes[0].stop_code !== 'NRW' && schedule.stopTimes[schedule.stopTimes.length - 1].stop_code !== 'NRW') {
                        return Accessibility.NO;
                    }
                }
            }
        }
        
        // If it is a Great Northern train starting / ending at Moorgate, leave it as unknown as bikes are not allowed
        // into the tunnel, but may still be allowed out of it
        if (schedule.stopAtStation('MOG') !== undefined) {
          return Accessibility.UNKNOWN;
        }

        // All the other trains should allow bikes, although a booking may be required
        return Accessibility.YES;
      })();

      trips.write(await schedule.toTrip(serviceId, routeId, this.repository, bikesAllowed));
      schedule.stopTimes.filter(r =>
          r.stop_code !== null // filter out technical stops at non-station
          && (r.departure_time != null || r.arrival_time != null) // filter out non-public stops
      )
          .map((r, index) => Object.assign(r, {stop_sequence: index}))
          .forEach(r => {
            const {stop_code, tiploc_code, ...remaining} = r;
            stopTimes.write(remaining);
          });
      const shapeId = schedule.getShapeId();
      if (!writtenShapes.has(shapeId)) {
        writtenShapes.add(shapeId);
        for (const record of await schedule.toShape(this.repository)) {
          shapes.write(record);
        }
      }
    }

    for (const route of Object.values(routes)) {
      if (route.route_type === RouteType.Rail && ['=LO', '=XR', '=ME'].includes(route.agency_id)) {
        route.route_type = RouteType.SuburbanRail;
      }
      routeFile.write(route);
    }

    trips.end();
    stopTimes.end();
    routeFile.end();
    shapes.end();

    return Promise.all([
      streamToPromise(trips),
      streamToPromise(stopTimes),
      streamToPromise(routeFile),
      streamToPromise(shapes),
    ]);
  }

  private async getSchedules(associations: Association[], scheduleResults: ScheduleResults): Promise<Schedule[]> {
    const processedAssociations = <AssociationIndex>applyOverlays(associations);
    const processedSchedules = <ScheduleIndex>applyOverlays(scheduleResults.schedules, scheduleResults.idGenerator);
    const associatedSchedules = applyAssociations(processedSchedules, processedAssociations, scheduleResults.idGenerator);
    const mergedSchedules = <Schedule[]>mergeSchedules(associatedSchedules);
    await Promise.all(mergedSchedules.map(schedule => ScheduleBuilder.fillStopHeadsigns(schedule, this.repository)));
    const schedules = addLateNightServices(mergedSchedules, scheduleResults.idGenerator);

    return schedules;
  }

}
