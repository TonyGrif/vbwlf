"""This DAG aquires USGS data at regular time intrevals"""

from typing import Dict
from airflow.sdk import dag, task
from datetime import datetime, timedelta
import yaml

from usgs import get_instantaneous_values


def read_config(path: str) -> Dict:
    """Read in configuration file to dictionary"""
    with open(path, mode="r") as f:
        return yaml.safe_load(f)


@dag(
    "USGS",
    description="ETL pipeline for USGS instantaneous values",
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    tags=["usgs", "etl"],
)
def usgs_instantaneous_values():
    @task(task_id="extract", retries=3, retry_delay=timedelta(minutes=3))
    def extract():
        # Check db for instances
        # If none, get historic data and populate to current
        # Else, query data from latest in db -> current

        # TODO: configure path location via CLI
        # TODO: work with volume
        config = read_config("./config.yaml")

        # Populate with historic data
        start = datetime.fromisoformat(config["start_date"])

        while (datetime.now() - start).days > 365:
            end = start.replace(year=start.year + 1)
            df = get_instantaneous_values(
                site_id="0204295505",
                params=["62620", "00045", "00036", "00035"],
                start_date=start.isoformat(),
                end_date=end.isoformat(),
            )
            print(df)

            start = end

        df = get_instantaneous_values(
            site_id="0204295505",
            params=["62620", "00045", "00036", "00035"],
            start_date=start.isoformat(),
        )
        print(df)

    extract()


usgs_instantaneous_values()
