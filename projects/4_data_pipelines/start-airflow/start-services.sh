#!/bin/bash
# Start services that airflow depend on
# Start postgres
service postgresql start
# Start cassandra
cassandra -f -R > /var/log/cassandra.log 2>&1 &
