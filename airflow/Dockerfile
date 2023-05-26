FROM apache/airflow:2.5.2
COPY requirements.txt .
RUN pip install -r requirements.txt
RUN pip install apache-airflow[aws,apache.spark]