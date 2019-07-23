#!/bin/bash

#
# Wojciech Golab, 2017
#

source ./settings.sh

${KAFKA_HOME}/bin/kafka-console-consumer.sh \
    --bootstrap-server $KBROKERS \
    --topic wordcount-output-bdrami \
    --property print.key=true \
    --property print.value=true \
    --property key.separator=, \
    --from-beginning \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer