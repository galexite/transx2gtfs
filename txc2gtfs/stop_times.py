from typing import Literal, cast

import pandas as pd


def get_direction(direction_id: str) -> Literal[0] | Literal[1]:
    """Return boolean direction id"""
    if direction_id == "inbound":
        return 0
    elif direction_id == "outbound":
        return 1

    raise ValueError(f"Cannot determine direction from {direction_id}")


def get_stop_times(gtfs_info: pd.DataFrame) -> pd.DataFrame:
    """Extract stop_times attributes from GTFS info DataFrame"""
    stop_times_cols = [
        "trip_id",
        "arrival_time",
        "departure_time",
        "stop_id",
        "stop_sequence",
        "timepoint",
    ]

    # Select columns
    stop_times = gtfs_info[stop_times_cols].copy()

    # Drop duplicates (there should not be any but make sure)
    stop_times = stop_times.drop_duplicates()

    # Ensure correct data types
    int_types = ["stop_sequence", "timepoint"]
    for col in int_types:
        stop_times[col] = stop_times[col].astype(int)

    # If there is only a single sequence for a trip, do not export it
    grouped = stop_times.groupby("trip_id")  # type: ignore
    filtered_stop_times = pd.DataFrame()
    for idx, group in grouped:
        if len(group) > 1:
            filtered_stop_times = pd.concat(
                [filtered_stop_times, group], ignore_index=True, sort=False
            )
        else:
            print(
                f"Trip {idx} does not include a sequence of stops, excluding from GTFS."
            )

    return filtered_stop_times


def generate_service_id(stop_times: pd.DataFrame) -> pd.DataFrame:
    """Generate service_id into stop_times DataFrame"""

    # Create column for service_id
    stop_times["service_id"] = None

    # Parse calendar info
    calendar_info = stop_times.drop_duplicates(subset=["vehicle_journey_id"])

    # Group by weekdays
    calendar_groups = calendar_info.groupby("weekdays")  # type: ignore

    # Iterate over groups and create a service_id
    for _, cgroup in calendar_groups:
        # Parse all vehicle journey ids
        vehicle_journey_ids = cast(list[str], cgroup["vehicle_journey_id"].to_list())

        # Parse other items
        service_ref = cgroup["service_ref"].unique()[0]
        daygroup = cgroup["weekdays"].unique()[0]
        start_d = cgroup["start_date"].unique()[0]
        end_d = cgroup["end_date"].unique()[0]

        # Generate service_id
        service_id = f"{service_ref}_{start_d}_{end_d}_{daygroup}"

        # Update stop_times service_id
        stop_times.loc[
            stop_times["vehicle_journey_id"].isin(vehicle_journey_ids), "service_id"  # type: ignore
        ] = service_id
    return stop_times
