FROM apache/airflow:2.5.0-python3.10

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow

COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

COPY DAGS/ /opt/airflow/dags/
COPY CONFIGURATIONS/ /opt/airflow/configurations/
