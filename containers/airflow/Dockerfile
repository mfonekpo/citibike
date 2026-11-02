FROM apache/airflow:3.3.2-python3.12

USER root

RUN mkdir -p /opt/citibike-venv /opt/citibike \
    && chown airflow:root /opt/citibike-venv /opt/citibike

USER airflow

COPY --chown=airflow:root requirements.txt \
    /tmp/requirements.txt

RUN python -m venv /opt/citibike-venv \
    && PIP_USER=false /opt/citibike-venv/bin/python -m pip install \
        --no-cache-dir \
        -r /tmp/requirements.txt \
    && /opt/citibike-venv/bin/python -m pip check \
    && /opt/citibike-venv/bin/dbt --version

ENV PYTHONDONTWRITEBYTECODE=1