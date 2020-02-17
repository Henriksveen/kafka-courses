#!/bin/bash

echo "Create topic nobill-call-record" 
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic nobill-call-record
echo "Create topic nobill-call-record-hour" 
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic nobill-call-record-hour
echo "Create topic nobill-call-record-day" 
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic nobill-call-record-day


echo "List all topics" 
kafka-topics --bootstrap-server localhost:9092 --list