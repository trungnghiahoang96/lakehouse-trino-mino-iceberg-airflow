# FROM apache/airflow:2.6.3
# FROM apache/airflow:2.9.0
FROM apache/airflow:2.7.1-python3.9

COPY requirements.txt /

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt