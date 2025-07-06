from collections.abc import Generator

import pandas as pd

from .util.network import download_cached
from .util.xml import NS, XMLTree

_NAPTAN_CSV_URL = "https://beta-naptan.dft.gov.uk/Download/National/csv"
_COLUMNS = {
    "ATCOCode": "stop_id",
    "Longitude": "stop_lon",
    "Latitude": "stop_lat",
    "CommonName": "stop_name",
}


def read_naptan_stops() -> pd.DataFrame:
    """
    Reads NaPTAN stops, downloading them if necessary.
    """
    naptan_fp = download_cached(_NAPTAN_CSV_URL, "Stops.csv")

    stops = pd.read_csv(naptan_fp, header=0, usecols=_COLUMNS.keys(), low_memory=False)  # type: ignore

    # Rename required columns into GTFS format
    return stops.rename(columns=_COLUMNS)


def get_stops(data: XMLTree) -> pd.DataFrame:
    """Parse stop data from TransXchange elements"""

    stop_points = data.find("txc:StopPoints", NS)
    if stop_points is None:
        raise ValueError("No StopPoints element. Could not parse stop information.")

    # Get stop database
    naptan_stops = read_naptan_stops()

    def gen_stoppoint_ids() -> Generator[str, None, None]:
        for point in stop_points.iterfind("./txc:StopPoints/txc:StopPoint", NS):
            # Name of the stop
            stop_name_el = point.find("./txc:Descriptor/txc:CommonName", NS)
            assert stop_name_el is not None, "No CommonName for StopPoint"
            stop_name = stop_name_el.text
            assert stop_name, "Empty CommonName for StopPoint"

            # Stop_id
            stop_id_el = point.find("./txc:AtcoCode", NS)
            assert stop_id_el is not None, "No AtcoCode for StopPoint"
            stop_id = stop_id_el.text
            assert stop_id, "Empty AtcoCode for StopPoint"
            yield stop_id

    def gen_annotatedstoppoint_ids() -> Generator[str, None, None]:
        # Iterate over stop points using TransXchange version 2.1
        for point in stop_points.iterfind("./txc:AnnotatedStopPointRef", NS):
            # Stop_id
            stop_ref_el = point.find("./txc:StopPointRef", NS)
            assert stop_ref_el is not None, "Invalid AnnotatedStopPointRef"
            stop_id = stop_ref_el.text
            assert stop_id, "Empty StopPointRef"
            yield stop_id

    if stop_points.find("txc:StopPoint", NS) is not None:
        stop_ids = list(gen_stoppoint_ids())
    elif stop_points.find("txc:AnnotatedStopPointRef", NS):
        stop_ids = list(gen_annotatedstoppoint_ids())
    else:
        raise ValueError("No StopPoint or AnnotatedStopPointRef elements.")

    return naptan_stops[naptan_stops["stop_id"].isin(stop_ids)]  # type: ignore
