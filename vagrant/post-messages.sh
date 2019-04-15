#!/bin/sh

vagrant ssh -- sudo -u kafka /usr/share/kafka_2.12-2.1.0/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic $1
