"""This DAG aquires USGS data at regular time intrevals"""

from airflow.sdk import dag, task
from datetime import datetime, timedelta

from usgs import query_instantaneous_values


@dag(
    "USGS",
    description="Request USGS instantaneous values",
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    tags=["usgs", "etl"],
)
def usgs_instantaneous_values():
    @task(task_id="extract", retries=3, retry_delay=timedelta(minutes=3))
    def extract():
        res = query_instantaneous_values(
            "0204295505",
            ["62620", "00045"],
            "2022-02-22",
            "2022-02-23",
            timeout=10,
        )
        print(res)

    extract()


usgs_instantaneous_values()
