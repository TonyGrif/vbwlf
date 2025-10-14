import pytest
from usgs.parser import query_instantaneous_values, parse_instantaneous_values

import pandas as pd


@pytest.fixture(scope="class")
def iv_response():
    return query_instantaneous_values(
        "0204295505", ["62620", "00045", "00036", "00035"], "2022-02-22", "2022-02-23"
    )


@pytest.fixture(scope="class")
def single_iv_response():
    return query_instantaneous_values("0204295505", "00045", "2022-02-22", "2022-02-23")


class TestUSGS:
    def test_query_iv(self, iv_response, single_iv_response):
        assert iv_response.status_code == 200
        assert iv_response.text is not None

        assert single_iv_response.status_code == 200
        assert single_iv_response.text is not None

    def test_parse_iv(self, iv_response, single_iv_response):
        data = iv_response.json()
        assert isinstance(data, dict)

        df = parse_instantaneous_values(data)
        assert isinstance(df.index, pd.DatetimeIndex)
        codes = ["62620", "00045", "00036", "00035"]
        assert all(col in df.columns for col in codes)
        assert len(df["62620"]) != 0
        assert len(df["00045"]) != 0
        assert len(df["00036"]) != 0
        assert len(df["00035"]) != 0

        data = single_iv_response.json()
        assert isinstance(data, dict)

        df = parse_instantaneous_values(data)
        assert isinstance(df.index, pd.DatetimeIndex)
        assert "00045" in df.columns
        assert len(df["00045"]) != 0
