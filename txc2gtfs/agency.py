from __future__ import annotations

from collections.abc import Generator

import pandas as pd

from txc2gtfs.util.xml import NS, XMLTree


def get_agency(data: XMLTree) -> pd.DataFrame:
    """Parse agency information from TransXchange elements"""

    def gen_agencies() -> Generator[tuple[str, str], None, None]:
        # Agency id
        for operator_el in data.iterfind("./txc:Operators/txc:Operator", NS):
            agency_id = operator_el.get("id")
            assert agency_id

            # Agency name
            agency_name_el = operator_el.find("txc:TradingName", NS)
            assert agency_name_el is not None
            agency_name = agency_name_el.text
            assert agency_name

            yield (
                agency_id,
                agency_name,
            )

    df = pd.DataFrame.from_records(
        gen_agencies(), columns=["agency_id", "agency_name"]
    ).drop_duplicates(ignore_index=True)
    df.loc[:, "agency_url"] = "NA"
    df.loc[:, "agency_timezone"] = "Europe/London"
    df.loc[:, "agency_lang"] = "en"

    return df
