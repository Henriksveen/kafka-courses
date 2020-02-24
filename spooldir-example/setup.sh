#!/bin/bash

# kafka-streams-application-reset --application-id nobill-extract-step --input-topics nobill-call-record,nobill-call-record-day,nobill-call-record-day --bootstrap-servers localhost:9092

echo "Create topic nobill-call-record" 
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic nobill-call-record
echo "Create topic nobill-call-record-hour" 
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic nobill-call-record-hour
echo "Create topic nobill-call-record-day" 
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic nobill-call-record-day
echo "Create topic nobill-sms-record" 
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic nobill-sms-record
echo "Create topic nobill-sms-record-hour" 
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic nobill-sms-record-hour
echo "Create topic nobill-sms-record-day" 
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic nobill-sms-record-day
echo "Create topic nobill-pads-record" 
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic nobill-pads-record
echo "Create topic nobill-pads-record-hour" 
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic nobill-pads-record-hour
echo "Create topic nobill-pads-record-day" 
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic nobill-pads-record-day


echo "List all topics" 
kafka-topics --bootstrap-server localhost:9092 --list