import pandas as pd

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


def get_agency(data) -> pd.Series:
    """Parse agency information from TransXchange elements"""
    # Agency id
    agency_id = data.TransXChange.Operators.Operator.get_attribute("id")

    # Agency name
    agency_name = data.TransXChange.Operators.Operator.OperatorNameOnLicence.cdata

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
