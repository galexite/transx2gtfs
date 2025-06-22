from __future__ import annotations

import pandas as pd

from transx2gtfs.dataio import XMLTree
from transx2gtfs.xml import NS

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


def get_agency(data: XMLTree) -> pd.Series[str]:
    """Parse agency information from TransXchange elements"""
    # Agency id
    operator_el = data.find("./txc:Operators/txc:Operator", NS)
    assert operator_el is not None
    agency_id = operator_el.get("id")
    assert agency_id

    # Agency name
    agency_name_el = operator_el.find("./txc:OperatorNameOnLicence", NS)
    assert agency_name_el is not None
    agency_name = agency_name_el.text
    assert agency_name

    # Agency url
    agency_url = _OPERATOR_URLS.get(agency_id, "NA")

    # Parse row
    return pd.Series(
        {
            "agency_id": agency_id,
            "agency_name": agency_name,
            "agency_url": agency_url,
            "agency_timezone": "Europe/London",
            "agency_lang": "en",
        }
    )
