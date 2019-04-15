#!/bin/sh

vagrant ssh -- sudo -u kafka /usr/share/kafka_2.12-2.2.0/bin/kafka-configs.sh --zookeeper localhost:2181  --entity-type topics --entity-name $1 --alter --add-config retention.ms=$2
