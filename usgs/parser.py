"""This module contains code for request and parsing USGS data

Attributes:
    query_instantaneous_values: Send an HTTP request for a site's instantaneous values
"""

import requests
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)


_SITE_URL = "https://waterservices.usgs.gov/nwis/iv/"


def query_instantaneous_values(
    site_id: str,
    params: str | List[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    agency: Optional[str] = "USGS",
    **kwargs,
) -> requests.Response:
    """Query the USGS instantaneous data in JSON format

    Args:
        site_id: USGS site identification number
        params: Parameter id(s) of desired data types in query
        start_date: ISO 8601 string representing the start date of query
        end_date: ISO 8601 string representing the end date of query
        agency: Agency in control of this site, defaults to USGS
        kwargs: Additional arguments passed to `requests.get`

    Returns:
        A `requests.Response` object upon successful response

    Throws:
        Raises `requests.HTTPError` on bad status code
    """

    format = "json"
    paramstr = ",".join(params)

    url = f"{_SITE_URL}?sites={site_id}&agencyCd={agency}&parameterCd={paramstr}&format={format}"

    if start_date is not None:
        url += f"&startDT={start_date}"
    if end_date is not None:
        url += f"&endDT={start_date}"

    logger.info("Making request with url: %s", url)

    res = requests.get(url, **kwargs)
    res.raise_for_status()
    return res
