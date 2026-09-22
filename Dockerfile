FROM apache/airflow:2.9.3
RUN pip install --no-cache-dir "dbt-postgres==1.9.0"