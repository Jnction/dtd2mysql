import * as chai from "chai";
import moment = require("moment");
import {Days, OverlapType, ScheduleCalendar} from "../../../src/gtfs/native/ScheduleCalendar";

describe("ScheduleCalendar", () => {

  it("detects overlaps", () => {
    const perm = calendar("2017-01-01", "2017-01-31");
    const underlay = calendar("2016-12-05", "2017-01-09");
    const innerlay = calendar("2017-01-05", "2017-01-07");
    const overlay = calendar("2017-01-31", "2017-02-07");
    const nolay = calendar("2017-02-05", "2017-02-07");

    chai.expect(perm.getOverlap(underlay)).to.deep.equal(OverlapType.Short);
    chai.expect(perm.getOverlap(innerlay)).to.deep.equal(OverlapType.Short);
    chai.expect(perm.getOverlap(overlay)).to.deep.equal(OverlapType.Short);
    chai.expect(perm.getOverlap(nolay)).to.deep.equal(OverlapType.None);
  });

  it("does not detect overlaps when the days don't match", () => {
    const weekday = calendar("2017-01-01", "2017-01-31", { 0: 1, 1: 1, 2: 1, 3: 1, 4: 1, 5: 1, 6: 0 });
    const weekend = calendar("2017-01-01", "2017-01-31", { 0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 1 });
    const tuesday = calendar("2017-01-01", "2017-01-31", { 0: 0, 1: 0, 2: 1, 3: 0, 4: 0, 5: 0, 6: 0 });

    chai.expect(weekday.getOverlap(weekend)).to.deep.equal(OverlapType.None);
    chai.expect(weekend.getOverlap(weekday)).to.deep.equal(OverlapType.None);
    chai.expect(weekday.getOverlap(tuesday)).to.deep.equal(OverlapType.Short);
  });

  it("detects short overlays", () => {
    const perm = calendar("2017-01-01", "2017-01-31");
    // Wed + Thurs for two weeks
    const short = calendar("2017-01-11", "2017-01-19", { 0: 0, 1: 0, 2: 0, 3: 1, 4: 1, 5: 0, 6: 0 });
    // full two weeks
    const long = calendar("2017-01-11", "2017-01-19");

    chai.expect(perm.getOverlap(short)).to.deep.equal(OverlapType.Short);
    chai.expect(perm.getOverlap(long)).to.deep.equal(OverlapType.Short);
  });

  it("adds exclude days", () => {
    const perm = calendar("2017-01-01", "2017-01-31");
    const overlay = calendar("2017-01-20", "2017-01-21");

    const calendar1 = perm.addExcludeDays(overlay);
    const excludeDays = Object.keys(calendar1!.excludeDays);

    chai.expect(excludeDays[0]).to.equal("20170120");
    chai.expect(excludeDays[1]).to.equal("20170121");
  });

  it("adds exclude days only within the range of the original date range", () => {
    const perm = calendar("2017-01-05", "2017-01-31");
    const underlay = calendar("2017-01-01", "2017-01-07");
    const overlay = calendar("2017-01-30", "2017-02-07");

    const calendar1 = perm.addExcludeDays(underlay);
    const calendar2 = calendar1!.addExcludeDays(overlay);
    const excludeDays = Object.keys(calendar2!.excludeDays);

    chai.expect(excludeDays.length).to.equal(0);
    chai.expect(calendar2!.runsFrom.isSame("20170108")).to.be.true;
    chai.expect(calendar2!.runsTo.isSame("20170129")).to.be.true;
  });

  it("adding exclude days might remove the schedule", () => {
    const perm = calendar("2017-01-01", "2017-01-15", { 0: 1, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0 });
    const c1 = calendar("2017-01-01", "2017-01-07", { 0: 1, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0 });
    const c2 = calendar("2017-01-08", "2017-01-15", { 0: 1, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0 });

    const calendar1 = perm.addExcludeDays(c1)!;

    chai.expect(calendar1.runsFrom.isSame("20170108")).to.be.true;
    chai.expect(calendar1.runsTo.isSame("20170115")).to.be.true;

    const calendars = calendar1.addExcludeDays(c2);

    chai.expect(calendars).null;
  });

  it("shift forward", () => {
    // Monday + Saturday service
    const c1 = calendar("2017-07-03", "2017-07-14", { 0: 0, 1: 1, 2: 0, 3: 0, 4: 0, 5: 0, 6: 1 });
    c1.excludeDays["20170710"] = moment("20170710");

    const c2 = c1.shiftForward();

    chai.expect(c2.days).to.deep.equal({ 0: 1, 1: 0, 2: 1, 3: 0, 4: 0, 5: 0, 6: 0 });
    chai.expect(c2.runsFrom.isSame("20170704")).to.be.true;
    chai.expect(c2.runsTo.isSame("20170715")).to.be.true;
    chai.expect(c2.excludeDays["20170710"]).to.be.undefined;
    chai.expect(c2.excludeDays["20170711"]).to.not.be.undefined;
  });

  it("shift backward", () => {
    // Sunday + Friday service
    const c1 = calendar("2017-07-02", "2017-07-14", { 0: 1, 1: 0, 2: 0, 3: 0, 4: 0, 5: 1, 6: 0 });
    c1.excludeDays["20170709"] = moment("20170709");

    const c2 = c1.shiftBackward();

    chai.expect(c2.days).to.deep.equal({ 0: 0, 1: 0, 2: 0, 3: 0, 4: 1, 5: 0, 6: 1 });
    chai.expect(c2.runsFrom.isSame("20170701")).to.be.true;
    chai.expect(c2.runsTo.isSame("20170713")).to.be.true;
    chai.expect(c2.excludeDays["20170709"]).to.be.undefined;
    chai.expect(c2.excludeDays["20170708"]).to.not.be.undefined;
  });

});

function calendar(from: string, to: string, days: Days = { 0: 1, 1: 1, 2: 1, 3: 1, 4: 1, 5: 1, 6: 1 }): ScheduleCalendar {
  return new ScheduleCalendar(
    moment(from),
    moment(to),
    days,
    {}
  );
}