from __future__ import annotations

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    import pandas as pd


def get_trips(gtfs_info: pd.DataFrame) -> pd.DataFrame:
    """Extract trips attributes from GTFS info DataFrame"""
    trip_cols = ["route_id", "service_id", "trip_id", "trip_headsign", "direction_id"]

    # Extract trips from GTFS info
    trips = gtfs_info.drop_duplicates(subset=["route_id", "service_id", "trip_id"])
    trips = trips[trip_cols].copy()
    trips = trips.reset_index(drop=True)

    # Ensure correct data types
    trips["direction_id"] = trips["direction_id"].astype(int)

    return trips
