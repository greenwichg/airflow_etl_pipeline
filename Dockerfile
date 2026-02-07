FROM apache/airflow:2.5.0-python3.10

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow

COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Preserve project directory structure so DAG can resolve
# ../config/region_config.json relative to its own path.
COPY dags/ /opt/airflow/project/dags/
COPY config/ /opt/airflow/project/config/

# Symlink DAGs into Airflow's dags folder
USER root
RUN ln -s /opt/airflow/project/dags/retail_sales_etl_dag.py /opt/airflow/dags/retail_sales_etl_dag.py
USER airflow
