"""This module contains code for request and parsing USGS data

Attributes:
    get_instantaneous_values: Get instantaneous values from a USGS site location
    query_instantaneous_values: Send an HTTP request for a site's instantaneous values
    parse_instantaneous_values: Parse the JSON response from USGS into a DataFrame
"""

import requests
from typing import Dict, List, Optional
import logging

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


_SITE_URL = "https://waterservices.usgs.gov/nwis/iv/"


def get_instantaneous_values(
    site_id: str,
    params: str | List[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    agency: Optional[str] = "USGS",
    **kwargs,
) -> pd.DataFrame:
    """Get the instantaneous values from a USGS site

    Args:
        site_id: USGS site identification number
        params: Parameter id(s) of desired data types in query
        start_date: ISO 8601 string representing the start date of query
        end_date: ISO 8601 string representing the end date of query
        agency: Agency in control of this site, defaults to USGS
        kwargs: Additional arguments passed to `requests.get`

    Returns:
        Pandas DataFrame
    """
    response = query_instantaneous_values(
        site_id, params, start_date, end_date, agency, **kwargs
    )
    return parse_instantaneous_values(response.json())


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

    if isinstance(params, list):
        paramstr = ",".join(params)
    else:
        paramstr = params

    url = f"{_SITE_URL}?sites={site_id}&agencyCd={agency}&parameterCd={paramstr}&format={format}"

    if start_date is not None:
        url += f"&startDT={start_date}"
    if end_date is not None:
        url += f"&endDT={end_date}"

    logger.info("Making request with url: %s", url)

    res = requests.get(url, **kwargs)
    res.raise_for_status()
    return res


def parse_instantaneous_values(data: Dict) -> pd.DataFrame:
    """Parse rdb string containing USGS instantaneous values

    Args:
        data: Dictionary from a USGS instantaneous value query

    Returns:
        Pandas DataFrame
    """
    disclaimer = data["value"]["queryInfo"].get("note")
    disclaimer = [val for val in disclaimer if val["title"] == "disclaimer"]
    if disclaimer:
        logger.warning("%s", disclaimer[0]["value"])

    site = data["value"]["queryInfo"]["criteria"]["locationParam"]

    dataframes = []
    empty_vars = []

    ts_data = data["value"]["timeSeries"]
    for param_val in ts_data:
        var_code = param_val["variable"]["variableCode"][0]["value"]
        values = param_val["values"][0]["value"]

        if values == []:
            logger.error(
                "No data found for parameter code %s, adding NaN column", var_code
            )
            empty_vars.append(var_code)
            continue

        df = pd.DataFrame(values)

        # TODO: Pull in column renaming here from configuration
        df = df.rename(columns={"dateTime": "datetime", "value": var_code})

        # NOTE: Dropping qualifiers from dataset
        df = df.drop(columns=["qualifiers"])
        # df["qualifiers"] = df["qualifiers"].apply(lambda x: x[0] if x else None)

        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.set_index("datetime")
        dataframes.append(df)
        logger.debug("Values (%s) aquired from %s", len(df), var_code)

    df = pd.concat(dataframes, axis=1)

    for empty in empty_vars:
        df[empty] = np.nan

    logger.info("Returning DataFrame (size: %s) from site %s", len(df), site)
    return df
