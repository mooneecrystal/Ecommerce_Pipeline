FROM apache/airflow:3.1.6

USER root
# Install git (required for dbt packages)
RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean

USER airflow

# 1. Install Airflow Providers (Snowflake) into the MAIN environment
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --user --no-cache-dir \
    -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.6/constraints-3.12.txt"

# 2. Install dbt in a SEPARATE Virtual Environment
# This prevents "Dependency Hell" between Airflow and dbt
RUN python -m venv /home/airflow/dbt_venv && \
    /home/airflow/dbt_venv/bin/pip install --no-cache-dir \
    dbt-snowflake>=1.7.0 \
    dbt-core>=1.7.0
    
COPY dbt_project/packages.yml /dbt_packages.yml
RUN dbt deps --profiles-dir .