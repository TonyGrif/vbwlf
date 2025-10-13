from usgs import query_instantaneous_values


class TestUSGS:
    def test_query_iv(self):
        res = query_instantaneous_values(
            "0204295505", ["62620", "00045"], "2022-02-22", "2022-02-23"
        )
        assert res.status_code == 200
        assert res.text is not None
