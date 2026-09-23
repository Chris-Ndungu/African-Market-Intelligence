FROM apache/airflow:3.3.1

# Switch to root to install system dependencies if needed
USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user for pip installs
USER airflow

# Pin exact versions — same as your local venv
RUN pip install --no-cache-dir \
    dbt-core==1.11.11 \
    dbt-bigquery==1.11.1