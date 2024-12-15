#!/bin/bash

host=$1
port=$2
shift 2
until nc -z -v -w30 $host $port
do
  echo "Waiting for $host:$port..."
  sleep 5
done
echo "$host:$port is ready"
exec "$@"
