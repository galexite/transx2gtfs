from collections.abc import Generator
from pathlib import Path
import pandas as pd
import pyproj
import warnings

from .util.network import download_cached
from .util.xml import NS, XMLTree, XMLElement


_NAPTAN_CSV_URL = "https://beta-naptan.dft.gov.uk/Download/National/csv"


def read_naptan_stops(naptan_fp: Path | None = None) -> pd.DataFrame:
    """
    Reads NaPTAN stops from temp. If the Stops do not exist in the temp, downloads the data.
    """
    if naptan_fp is None:
        naptan_fp = download_cached(_NAPTAN_CSV_URL, "Stops.csv")

    stops = pd.read_csv(naptan_fp, low_memory=False) # type: ignore

    # Rename required columns into GTFS format
    stops = stops.rename(
        columns={
            "ATCOCode": "stop_id",
            "Longitude": "stop_lon",
            "Latitude": "stop_lat",
            "CommonName": "stop_name",
        }
    )

    # Keep only required columns
    required_cols = ["stop_id", "stop_lon", "stop_lat", "stop_name"]
    for col in required_cols:
        if col not in stops.columns:
            raise ValueError(
                f"Required column {col} could not be found from stops DataFrame."
            )
    stops = stops[required_cols].copy()
    return stops


def _get_tfl_style_stops(stop_points: XMLElement) -> pd.DataFrame:
    """"""
    # Helper projections for transformations
    # Define the projection
    # The .srs here returns the Proj4-string presentation of the projection
    wgs84 = pyproj.Proj("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs")
    osgb36 = pyproj.Proj(
        "+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.999601 +x_0=400000 +y_0=-100000 +ellps=airy +towgs84=446.448,-125.157,542.060,0.1502,0.2470,0.8421,-20.4894 +units=m +no_defs <>"
    )

    # Attributes
    _stop_id_col = "stop_id"

    # Container
    stop_data = pd.DataFrame()

    # Get stop database
    naptan_stops = read_naptan_stops()

    # Iterate over stop points
    for point in stop_points.iterfind("./txc:StopPoints/txc:StopPoint", NS):
        # Name of the stop
        stop_name_el = point.find("./txc:Descriptor/txc:CommonName", NS)
        assert stop_name_el is not None, "No CommonName for StopPoint"
        stop_name = stop_name_el.text
        assert stop_name, "Empty CommonName for StopPoint"

        # Stop_id
        stop_id_el = point.find("txc:AtcoCode", NS)
        assert stop_id_el is not None, "No AtcoCode for StopPoint"
        stop_id = stop_id_el.text
        assert stop_id, "Empty AtcoCode for StopPoint"

        # Get stop info
        stop = naptan_stops.loc[naptan_stops[_stop_id_col] == stop_id]

        # If local NAPTAN db does not contain the info,
        # try to refresh local dump or parse from the data directly

        if len(stop) == 0:
            # Try first to refresh the Stop data
            # -----------------------------------
            naptan_stops = read_naptan_stops()
            stop = naptan_stops.loc[naptan_stops[_stop_id_col] == stop_id]

            if len(stop) == 0:
                # If was not found, try to read from TransXchange data directly
                # -------------------------------------------------------------
                try:
                    # X and y coordinates - Notice: these might not be available! --> Use NAPTAN database
                    # Spatial reference - TransXChange might use:
                    #   - OSGB36 (epsg:7405) spatial reference: https://spatialreference.org/ref/epsg/osgb36-british-national-grid-odn-height/
                    #   - WGS84 (epsg:4326)
                    # Detected epsg
                    detected_epsg = None
                    x_el = point.find("./txc:Place/txc:Location/txc:Easting", NS)
                    assert x_el is not None
                    x_str = x_el.text
                    assert x_str
                    x = float(x_str)
                    y_el = point.find("./txc:Place/txc:Location/txc:Northing", NS)
                    assert y_el is not None
                    y_str = y_el.text
                    assert y_str
                    y = float(y_str)

                    # Detect the most probable CRS at the first iteration
                    if detected_epsg is None:
                        # Check if the coordinates are in meters
                        if x > 180:
                            detected_epsg = 7405
                        else:
                            detected_epsg = 4326

                    # Convert point coordinates to WGS84 if they are in OSGB36
                    if detected_epsg == 7405:
                        x, y, _, _ = pyproj.transform(p1=osgb36, p2=wgs84, x=x, y=y)

                    # Create row
                    stop = pd.Series({
                        "stop_id": stop_id,
                        "stop_code": None,
                        "stop_name": stop_name,
                        "stop_lat": y,
                        "stop_lon": x,
                        "stop_url": None,
                    })

                except Exception:
                    warnings.warn(
                        f"Did not find a NaPTAN stop for '{stop_id}'",
                        UserWarning,
                        stacklevel=2,
                    )
                    continue

        elif len(stop) > 1:
            raise ValueError("Had more than 1 stop with identical stop reference.")

        # Add to container
        stop_data = pd.concat([stop_data, stop])

    return stop_data


def _get_txc_21_style_stops(stop_points: XMLElement) -> pd.DataFrame:
    # Get stop database
    naptan_stops = read_naptan_stops()

    def gen_stop_ids() -> Generator[str, None, None]:
        # Iterate over stop points using TransXchange version 2.1
        for point in stop_points.iterfind("./txc:AnnotatedStopPointRef", NS):
            # Stop_id
            stop_ref_el = point.find("txc:StopPointRef", NS)
            assert stop_ref_el is not None, "Invalid AnnotatedStopPointRef"
            stop_id = stop_ref_el.text
            assert stop_id, "Empty StopPointRef"
            yield stop_id

    stop_ids = list(gen_stop_ids())

    # Get stop info
    return naptan_stops[naptan_stops["stop_id"].isin(stop_ids)] # type: ignore


def get_stops(data: XMLTree) -> pd.DataFrame:
    """Parse stop data from TransXchange elements"""

    stop_points = data.find("txc:StopPoints", NS)
    if stop_points is None:
        raise ValueError(
            "No StopPoints element. Could not parse stop information."
        )

    if stop_points.find("txc:StopPoint", NS) is not None:
        stop_data = _get_tfl_style_stops(stop_points)
    elif stop_points.find("txc:AnnotatedStopPointRef", NS):
        stop_data = _get_txc_21_style_stops(stop_points)
    else:
        raise ValueError("No StopPoint or AnnotatedStopPointRef elements.")

    return stop_data
