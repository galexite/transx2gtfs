from __future__ import annotations
from collections.abc import Generator

import pandas as pd

from txc2gtfs.util.xml import NS, XMLTree

_OPERATOR_URLS = {
    "OId_LUL": "https://tfl.gov.uk/maps/track/tube",
    "OId_DLR": "https://tfl.gov.uk/modes/dlr/",
    "OId_TRS": "https://www.thamesriverservices.co.uk/",
    "OId_CCR": "https://www.citycruises.com/",
    "OId_CV": "https://www.thamesclippers.com/",
    "OId_WFF": "https://tfl.gov.uk/modes/river/woolwich-ferry",
    "OId_TCL": "https://tfl.gov.uk/modes/trams/",
    "OId_EAL": "https://www.emiratesairline.co.uk/",
}


def get_agency(data: XMLTree) -> pd.DataFrame:
    """Parse agency information from TransXchange elements"""

    def gen_agencies() -> Generator[tuple[str, str, str, str, str], None, None]:
        # Agency id
        for operator_el in data.iterfind("./txc:Operators/txc:Operator", NS):
            agency_id = operator_el.get("id")
            assert agency_id

            # Agency name
            agency_name_el = operator_el.find("./txc:OperatorNameOnLicence", NS)
            assert agency_name_el is not None
            agency_name = agency_name_el.text
            assert agency_name

            # Agency url
            agency_url = _OPERATOR_URLS.get(agency_id, "NA")

            yield (
                agency_id,
                agency_name,
                agency_url,
                "Europe/London",
                "en",
            )

    return pd.DataFrame.from_records( # type: ignore
        gen_agencies(),
        columns=[
            "agency_id",
            "agency_name",
            "agency_url",
            "agency_timezone",
            "agency_lang",
        ]
    ).drop_duplicates(ignore_index=True)
