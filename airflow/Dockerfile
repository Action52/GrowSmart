# First stage: Base on bitnami/spark:3.3
FROM bitnami/spark:3.3 AS spark-base

# Second stage: Base on apache/airflow:2.5.2
FROM apache/airflow:2.5.2

# Install necessary packages
USER root
RUN apt-get update -y && apt-get install -y procps wget openjdk-11-jdk
# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64/
RUN export JAVA_HOME
RUN export PATH=$PATH:$JAVA_HOME/bin

# Install Python 3.9
RUN apt-get install -y software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get install -y python3.9
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 1
RUN apt-get install -y python3.9-distutils python3.9-dev python3.9-venv
RUN curl https://bootstrap.pypa.io/get-pip.py | python3.9

# Copy /opt/bitnami/spark from the first stage
COPY --from=spark-base /opt/bitnami/spark /opt/bitnami/spark

ENV SPARK_HOME /opt/bitnami/spark
ENV PATH $PATH:${SPARK_HOME}/bin
ENV PYSPARK_PYTHON python3.9
ENV PYSPARK_DRIVER_PYTHON python3.9

USER airflow
ADD jars /home/airflow/.local/lib/python3.9/site-packages/pyspark/jars
RUN export PATH=$PATH:$JAVA_HOME/bin:/home/airflow/.local/lib/python3.7/site-packages/pyspark/jars
RUN pip install pyspark
RUN pip install apache-airflow[aws,apache.spark]
RUN pip install apache-airflow-providers-ssh
