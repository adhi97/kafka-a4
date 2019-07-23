unset JAVA_TOOL_OPTIONS
JAVA=java
JAVA_CC=javac

KAFKA_HOME="./kafka_2.11-2.3.0"
export CLASSPATH=.:"${KAFKA_HOME}/libs/*"
# changing ${USER} to bdrami
STATE_STORE_DIR=/tmp/WordCount-Kafka-state-store-bdrami

ZKSTRING=manta.uwaterloo.ca:2181
KBROKERS=manta.uwaterloo.ca:9092
STOPIC=student-bdrami
CTOPIC=classroom-bdrami
OTOPIC=output-bdrami
APP_NAME=WordCount-bdrami
