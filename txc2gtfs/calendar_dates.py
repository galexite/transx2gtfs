import warnings
from collections.abc import Generator, Iterable
from typing import cast

import pandas as pd

from txc2gtfs.bank_holidays import get_bank_holiday_dates
from txc2gtfs.util.xml import NS, XMLElement


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
    non_operative_values = (
        s for s in cast(Iterable[str], gtfs_info["non_operative_days"].unique()) if s
    )

    # Container for all info
    non_operatives = set(
        day for value in non_operative_values for day in value.split("|")
    )

    # Check if there exists some exceptions that are not known bank holidays
    for holiday in non_operatives:
        if (
            (holiday not in _KNOWN_HOLIDAYS.keys())
            and (holiday != "AllBankHolidays")
            and not holiday.endswith("Eve")
        ):
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

    # Iterate over services and produce rows having exception with given bank holiday dates
    def gen_calendar_dates() -> Generator[tuple[str, str, int], None, None]:
        for _, row in gtfs_info.drop_duplicates(subset=["service_id"]).iterrows():  # type: ignore
            # Iterate over exception dates
            for date in bank_holidays:
                # Generate row
                yield (
                    row["service_id"],
                    date,
                    2,
                )

    return pd.DataFrame.from_records(  # type: ignore
        gen_calendar_dates(), columns=["service_id", "date", "exception_type"]
    )
