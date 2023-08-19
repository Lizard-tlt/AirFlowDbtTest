FROM apache/airflow:2.7.0
ADD requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
