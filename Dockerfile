FROM apache/airflow:3.1.0
ENV AIRFLOW_USE_UV=true

USER airflow
WORKDIR /vbwlf

COPY pyproject.toml uv.lock README.md .
COPY usgs/ ./usgs

RUN uv sync --frozen --no-dev
