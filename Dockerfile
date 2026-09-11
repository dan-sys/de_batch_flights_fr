FROM apache/airflow:3.0.0
COPY requirements.txt /
COPY TokenManager.py /opt/airflow/TokenManager.py
RUN pip install --no-cache-dir -r /requirements.txt