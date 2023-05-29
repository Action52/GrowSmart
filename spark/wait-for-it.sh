#!/bin/bash

# Host and port to wait for
HOST=$1
PORT=$2

# Wait until the host is ready
while ! nc -z $HOST $PORT; do
  sleep 1
done

# Execute the command
shift 2
"$@"
