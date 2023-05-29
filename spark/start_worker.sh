#!/bin/bash

# Wait for 60 seconds
echo "Waiting for 60 seconds before starting the Spark worker..."
sleep 60

# Start the Spark worker
echo "Starting the Spark worker..."
/opt/bitnami/scripts/spark/run.sh $SPARK_MASTER_URL
