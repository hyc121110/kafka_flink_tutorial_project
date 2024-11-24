FROM openjdk:8u151-jdk-alpine3.7

# install bash
RUN apk add --no-cache bash libc6-compat

WORKDIR /
COPY wait-for-it.sh wait-for-it.sh
COPY target/flink-kafka2postgres-1.0-SNAPSHOT-jar-with-dependencies.jar flink-processor.jar

# wait for zookeeper and kafka to be available and run application
CMD ./wait-for-it.sh -s -t 30 $ZOOKEEPER_SERVER -- ./wait-for-it.sh -s -t 30 $KAFKA_SERVER -- java -Xmx512m -jar flink-processor.jar