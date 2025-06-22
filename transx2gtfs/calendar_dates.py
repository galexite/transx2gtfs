from collections.abc import Iterable
from typing import cast
import pandas as pd
from transx2gtfs.bank_holidays import get_bank_holiday_dates
import warnings

from transx2gtfs.util.xml import NS, XMLElement


def get_non_operation_days(data: XMLElement) -> str | None:
    """
    Get days of non-operation.
    """

    non_operation_days = data.findall(
        "./txc:OperatingProfile/txc:BankHolidayOperation/txc:DaysOfNonOperation/*", NS
    )
    if not non_operation_days:
        return None

    return "|".join(
        weekday.tag.rsplit("}", maxsplit=1)[1] for weekday in non_operation_days
    )


# Known exceptions and their counterparts in bankholiday table
_KNOWN_HOLIDAYS = {
    "SpringBank": "Spring bank holiday",
    "LateSummerBankHolidayNotScotland": "Summer bank holiday",
    "MayDay": "Early May bank holiday",
    "GoodFriday": "Good Friday",
    "EasterMonday": "Easter Monday",
    "BoxingDay": "Boxing Day",
    "ChristmasDay": "Christmas Day",
    "NewYearsDay": "New Year’s Day",
    "BoxingDayHoliday": "Boxing Day",
    "ChristmasDayHoliday": "Christmas Day",
    "NewYearsDayHoliday": "New Year’s Day",
}


def get_calendar_dates(gtfs_info: pd.DataFrame) -> pd.DataFrame | None:
    """
    Parse calendar dates attributes from GTFS info DataFrame.

    TransXChange typically indicates exception in operation using 'AllBankHolidays' as an attribute.
    Hence, Bank holiday information is retrieved from "https://www.gov.uk/" site that should keep the data up-to-date.
    If the file (or internet) is not available, a static version of the same file will be used that is bundled with the package.

    There are different bank holidays in different regions in UK.
    Available regions are: 'england-and-wales', 'scotland', 'northern-ireland'

    """
    # Get initial info about non-operative days
    gtfs_info = gtfs_info.copy()
    gtfs_info = gtfs_info.dropna(subset=["non_operative_days"])
    non_operative_values = (s for s in cast(Iterable[str], gtfs_info["non_operative_days"].unique()) if s)

    # Container for all info
    non_operatives = set(day for value in non_operative_values for day in value.split("|"))

    # Check if there exists some exceptions that are not known bank holidays
    for holiday in non_operatives:
        if (holiday not in _KNOWN_HOLIDAYS.keys()) and (holiday != "AllBankHolidays") and not holiday.endswith("Eve"):
            warnings.warn(
                f"Did not recognize holiday {holiday}",
                UserWarning,
                stacklevel=2,
            )

    if len(non_operatives) > 0:
        # Get bank holidays that are during the operative period of the feed
        bank_holidays = get_bank_holiday_dates(gtfs_info)
    else:
        return None

    # Return None if no bank holiday happens to be during the operative period
    if not bank_holidays:
        return None

    # Otherwise produce calendar_dates data

    # Select distinct (service_id) rows that have bank holiday determined
    calendar_info = gtfs_info[["service_id", "non_operative_days"]].copy()
    calendar_info = calendar_info.drop_duplicates(subset=["service_id"])

    # Create columns for date and exception_type
    calendar_info["date"] = None

    # The exception will always be indicating non-operative service (value 2)
    calendar_info["exception_type"] = 2

    # Container for calendar_dates
    calendar_info.apply(update_calendar_info)

    # Iterate over services and produce rows having exception with given bank holiday dates
    for idx, row in calendar_info.iterrows():
        # Iterate over exception dates
        for date in bank_holidays:
            # Generate row
            row = dict(
                service_id=row["service_id"],
                date=date,
                exception_type=row["exception_type"],
            )
            # Add to container
            calendar_dates = calendar_dates.append(row, ignore_index=True, sort=False)

    # Ensure correct datatype
    calendar_info["exception_type"] = calendar_dates["exception_type"].astype(int)

    return calendar_info
