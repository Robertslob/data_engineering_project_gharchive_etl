# Use the official Airflow image as a base
FROM apache/airflow:3.1.3

USER airflow

# Install extra Python packages
# Use --no-cache-dir to reduce image size
RUN pip install --no-cache-dir \
    boto3 \
    requests \
    pandas \
    snowflake-connector-python \
    apache-airflow-providers-amazon \
    apache-airflow-providers-snowflake \
    duckdb