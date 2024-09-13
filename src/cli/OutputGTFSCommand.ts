import * as moment from 'moment';
import {FeedInfo} from '../gtfs/file/FeedInfo';
import {Route} from '../gtfs/file/Route';
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
    const transfersP = this.copy(this.repository.getTransfers(), "transfers.txt");
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
    const trips = this.output.open(this.baseDir + "trips.txt");
    const stopTimes = this.output.open(this.baseDir + "stop_times.txt");
    const routeFile = this.output.open(this.baseDir + "routes.txt");
    const shapes = this.output.open(this.baseDir + "shapes.txt");
    const routes = {};
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

      trips.write(await schedule.toTrip(serviceId, routeId, this.repository));
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
