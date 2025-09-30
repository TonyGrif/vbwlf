# Virginia Beach Water Level Forecasting

## Running Instructions
* Create directories with the correct permissions
    * `mkdir -p ./dags ./logs ./plugins ./config`
    * `echo -e "AIRFLOW_UID=$(id -u)" > .env`
* Initialize the database for Airflow
    * `docker compose up airflow-init`
* Start all services
    * `docker compose up`
* Access the environment
    * Through the CLI
        * Run through docker: `docker compose run airflow-worker airflow {command}`
        * Run through script: `./airflow.sh {command}`
    * Through Web Interface:
        * `http://localhost:{port}`
        * Log in with username: `airflow`, password: `airflow`
    * Through REST API:
        * `curl {method} --user "{username}:{password} {endpoint}`
