
# A Fork of dtd2mysql to generate National Rail GTFS with better interoperability to UK national standards

An import tool for the British rail fares, routeing and timetable feeds into a database.

Although both the timetable and fares feed are open data you will need to obtain the fares feed via the [ATOC website](http://data.atoc.org/fares-data). The formal specification for the data inside the feed also available on the [ATOC website](http://data.atoc.org/sites/all/themes/atoc/files/SP0035.pdf).

At the moment only MySQL compatible databases are supported but it could be extended to support other data stores. PRs are very welcome.

## Changes to upstream

Pull requests are made to [upstream](https://github.com/planarnetwork/dtd2mysql) if applicable, however, the following are the main improvements
which differ significantly from upstream, all related to GTFS generation:

* `stops.txt` includes all stations and platforms, with ATCO code as the ID. This helps combining with other national GTFS datasets of other transport modes.
* `agency_id` are prefixed with `=` as in the Traveline NOC dataset.
* Each schedule entry in the timetable data is mapped to exactly one GTFS trip, with the `original_trip_id` field being the UID of the schedule entry.
* The scheduled platform allocation for each service is included.
* Adds the ability to supply alternative station data from an external JSON file, including coordinates and wheelchair accessibility.
* Adds the ability to supply extra stops (platforms) not found in the timetable data.
* The RSID is used for trip names.
* Names such as "Stansted Express", "West Midlands Railway", etc., are used for route names.
* Route colours are added.
* Shapes are generated for every trip from the TIPLOCs of the actual routing used, such that diversions can be shown on journey planners.
* Headsigns are filled by logic in order to replicate the departure boards in real life.
* Non-National Rail services, such as metro services, bus services (except rail replacement buses) and ship services are not included in the GTFS output.

## Fares 

Each of these commands relies on the database settings being set in the environment variables. For example `DATABASE_USERNAME=root DATABASE_NAME=fares dtd2mysql --fares-clean`.

### Import

Import the fares into a database, creating the schema if necessary. This operation is destructive and will remove any existing data.

```
dtd2mysql --fares /path/to/RJFAFxxx.ZIP
```
### Clean 

Removes expired data and invalid fares, corrects railcard passenger quantities, adds full date entries to restriction date records. This command will occasionally fail due to a MySQL timeout (depending on hardware), re-running the command should correct the problem.

```
dtd2mysql --fares-clean
```
## Timetables
### Import

Import the timetable information into a database, creating the schema if necessary. This operation is destructive and will remove any existing data.

```
dtd2mysql --timetable /path/to/RJTTFxxx.ZIP
```

### Convert to GTFS

Convert the DTD/TTIS version of the timetable (up to 3 months into the future) to GTFS. 

```
dtd2mysql --timetable /path/to/RJTTFxxx.ZIP
dtd2mysql --gtfs-zip filename-of-gtfs.zip

# use alternative source of station data
# the provided example contains station and platform coordinates extracted from OpenStreetMap
dtd2mysql --gtfs-zip filename-of-gtfs.zip stations.example.json
```

## Routeing Guide
### Import
```
dtd2mysql --routeing /path/to/RJRGxxxx.ZIP
# optional
dtd2mysql --nfm64 /path/to/nfm64.zip 
```

## Download from SFTP server

The download commands will take the latest full refresh from an SFTP server (by default the DTD server).

Requires the following environment variables:

```
SFTP_USERNAME=dtd_username
SFTP_PASSWORD=dtd_password
SFTP_HOSTNAME=dtd_hostname (this will default to dtd.atocrsp.org)
```

There is a command for each feed

```
dtd2mysql --download-fares /path/
dtd2mysql --download-timetable /path/
dtd2mysql --download-routeing /path/
dtd2mysql --download-nfm64 /path/
```

Or download and process in one command

```
dtd2mysql --get-fares
dtd2mysql --get-timetable
dtd2mysql --get-routeing
dtd2mysql --get-nfm64
```

## Notes
### null values

Values marked as all asterisks, empty spaces, or in the case of dates - zeros, are set to null. This is to preverse the integrity of the column type. For instance a route code is numerical although the data feed often uses ***** to signify any so this value is converted to null. 

### keys
Although every record format has a composite key defined in the specification an `id` field is added as the fields in the composite key are sometimes null. This is no longer supported in modern versions of MariaDB or MySQL.

### missing data
At present journey segments, class legends, rounding rules, print formats  and the fares data feed meta data are not imported. They are either deprecated or irrelevant. Raise an issue or PR if you would like them added.

### timetable format

The timetable data does not map to a relational database in a very logical fashion so all LO, LI and LT records map to a single `stop_time` table.

### GTFS feed cutoff date

Only schedule records that **start** up to 3 months into the future (using date of import as a reference point) are exported to GTFS for performance reasons.
This will cause any data after that point to be either incomplete or incorrect, as override/cancellation records after that will be ignored as well.

## Contributing

Issues and PRs are very welcome. To get the project set up run

```
git clone git@github.com:jnction/dtd2mysql
npm install --dev
npm test
```

If you would like to send a pull request please write your contribution in TypeScript and if possible, add a test.

## License

This software is licensed under [GNU GPLv3](https://www.gnu.org/licenses/gpl-3.0.en.html).

Copyright 2024 Linus Norton & Jnction Limited.

tiplocs.csv is derived from https://github.com/oweno-tfwm/YA_Tiploc_List under the same licence. 