"""
Convert transXchange data format to GTFS format.

The TransXChange model) has seven basic concepts: Service, Registration, Operator, Route,
StopPoint, JourneyPattern, and VehicleJourney.
    - A Service brings together the information about a registered bus service, and may contain
        two types of component service: Standard or Flexible; a mix of both types is allowed within a
        single Service.
    - A normal bus schedule is described by a StandardService and a Route. A Route describes
        the physical path taken by buses on the service as a set of route links.
    - A FlexibleService describes a bus service that does not have a fixed route, but only a
        catchment area or a few variable stops with no prescribed pattern of use.
    - A StandardService has one or more JourneyPattern elements to describe the common
        logical path of traversal of the stops of the Route as a sequence of timing links (see later),
        and one or more VehicleJourney elements, which describe individual scheduled journeys by
        buses over the Route and JourneyPattern at a specific time.
    - Both types of service have a registered Operator, who runs the service. Other associated
        operator roles can also be specified.
    - Route, JourneyPattern and VehicleJoumey follow a sequence of NaPTAN StopPoints. A
        Route specifies in effect an ordered list of StopPoints. A JourneyPattern specifies an
        ordered list of links between these points, giving relative times between each stop; a
        VehicleJourney follows the same list of stops at specific absolute passing times. (The
        detailed timing Link and elements that connect VehicleJourneys, JourneyPatterns etc to
        StopPoints are not shown in Figure 3-1). StopPoints may be grouped within StopAreas.
    - The StopPoints used in a JourneyPattern or Route are either declared locally or by
        referenced to an external definition using an AnnotatedStopRef
    - A Registration specifies the registration details for a service. It is mandatory in the
        registration schema.

Author
------
Dr. Henrikki Tenkanen, University College London

License
-------

MIT.
"""

from __future__ import annotations

import functools
import multiprocessing
import sqlite3
import xml.etree.ElementTree as ET
from collections.abc import Generator, Iterable
from pathlib import Path
from typing import TYPE_CHECKING

from txc2gtfs.agency import AgencyTable
from txc2gtfs.calendar import get_calendar
from txc2gtfs.calendar_dates import get_calendar_dates
from txc2gtfs.gtfs import export_to_zip
from txc2gtfs.routes import get_routes
from txc2gtfs.stop_times import get_stop_times
from txc2gtfs.stops import get_stops
from txc2gtfs.transxchange import get_gtfs_info
from txc2gtfs.trips import get_trips

if TYPE_CHECKING:
    from _typeshed import StrPath


def process_file(path: Path, gtfs_db: Path) -> None:
    # If type is string, it is a direct filepath to XML
    data = ET.parse(path)

    # Parse stops
    stop_data = get_stops(data)

    # Parse GTFS info containing data about trips, calendar, stop_times and calendar_dates
    gtfs_info = get_gtfs_info(data)

    # Parse stop_times
    stop_times = get_stop_times(gtfs_info)

    # Parse trips
    trips = get_trips(gtfs_info)

    # Parse calendar
    calendar = get_calendar(gtfs_info)

    # Parse calendar_dates
    calendar_dates = get_calendar_dates(gtfs_info)

    # Parse routes
    routes = get_routes(gtfs_info, data)

    # Initialize database connection
    with sqlite3.connect(gtfs_db) as conn:
        # Only export data into db if there exists valid stop_times data

        if len(stop_times) > 0:
            cur = conn.cursor()
            for cls in (AgencyTable,):
                table = cls(cur)
                table.populate(cur, data)
                conn.commit()

            stop_times.to_sql(
                name="stop_times", con=conn, index=False, if_exists="append"
            )
            stop_data.to_sql(name="stops", con=conn, index=False, if_exists="append")
            routes.to_sql(name="routes", con=conn, index=False, if_exists="append")
            trips.to_sql(name="trips", con=conn, index=False, if_exists="append")
            calendar.to_sql(name="calendar", con=conn, index=False, if_exists="append")

            if calendar_dates is not None:
                calendar_dates.to_sql(
                    name="calendar_dates", con=conn, index=False, if_exists="append"
                )
        else:
            print(
                f"UserWarning: File {path.name} did not contain valid stop_sequence data, skipping."
            )


def _iterate_paths(input: Iterable[StrPath]) -> Generator[Path, None, None]:
    for path in input:
        path = Path(path)
        if path.is_dir():
            yield from path.glob("*.xml")
            continue
        yield path


def convert(
    input: Iterable[StrPath],
    output: StrPath,
    append_to_existing: bool = False,
    num_workers: int = 1,
    file_size_limit: int = 2000,
) -> None:
    """
    Converts TransXchange formatted schedule data into GTFS feed.

    input_filepath : str
        File path to data directory or a ZipFile containing one or multiple TransXchange .xml files.
        Also nested ZipFiles are supported (i.e. a ZipFile with ZipFile(s) containing .xml files.)
    output_filepath : str
        Full filepath to the output GTFS zip-file, e.g. '/home/myuser/data/my_gtfs.zip'
    append_to_existing : bool (default is False)
        Flag for appending to existing gtfs-database. This might be useful if you have
        TransXchange .xml files distributed into multiple directories (e.g. separate files for
        train data, tube data and bus data) and you want to merge all those datasets into a single
        GTFS feed.
    worker_cnt : int
        Number of workers to distribute the conversion process. By default the number of CPUs is used.
    file_size_limit : int
        File size limit (in megabytes) can be used to skip larger-than-memory XML-files (should not happen).
    """
    input = _iterate_paths(input)
    output = Path(output)

    # Filepath for temporary gtfs db
    gtfs_db = output.parent / "gtfs.db"

    # If append to database is false remove previous gtfs-database if it exists
    if not append_to_existing:
        gtfs_db.unlink(missing_ok=True)

    # Create workers
    if num_workers > 1:
        with multiprocessing.Pool(num_workers) as pool:
            _process_file = functools.partial(process_file, gtfs_db=gtfs_db)
            pool.map(_process_file, input)
    else:
        for file in input:
            process_file(file, gtfs_db)

    export_to_zip(gtfs_db, output)
