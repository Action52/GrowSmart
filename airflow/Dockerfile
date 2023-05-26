FROM apache/airflow:2.5.3

USER root

# Install OpenJDK-11
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get install -y glibc-source && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64/
RUN export JAVA_HOME
RUN export PATH=$PATH:$JAVA_HOME/bin



USER airflow

COPY requirements.txt requirements.txt

ADD jars /home/airflow/.local/lib/python3.7/site-packages/pyspark/jars

USER root
RUN chmod -R 777 /home/airflow/.local/lib/python3.7/
USER airflow

RUN pip install --no-cache-dir -r requirements.txt

USER airflow

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64/
RUN export JAVA_HOME
RUN export PATH=$PATH:$JAVA_HOME/bin:/home/airflow/.local/lib/python3.7/site-packages/pyspark/jars
