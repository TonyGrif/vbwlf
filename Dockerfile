FROM apache/airflow:3.1.0
ENV AIRFLOW_USE_UV=true

USER airflow
WORKDIR /opt/airflow

RUN mkdir -p /opt/airflow/vbwlf/usgs

COPY pyproject.toml uv.lock README.md ./vbwlf
COPY usgs/ ./vbwlf/usgs

RUN uv pip install /opt/airflow/vbwlf
