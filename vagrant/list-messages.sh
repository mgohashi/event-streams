#!/bin/sh

vagrant ssh -- sudo -u kafka /usr/share/kafka_2.12-2.2.0/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $1 --from-beginning
