#!/usr/bin/env bash

SPARK_APPLICATION_JAR="project1SABD-1.0-SNAPSHOT-jar-with-dependencies.jar"
SPARK_APPLICATION_JAR_LOCATION=/${SPARK_APPLICATION_JAR}

docker-compose -f spark/docker-spark-cluster-master/docker-compose.yml up -d

docker cp spark/docker-spark-cluster-master/${SPARK_APPLICATION_JAR_LOCATION}  spark-master:${SPARK_APPLICATION_JAR_LOCATION}
docker cp spark/docker-spark-cluster-master/${SPARK_APPLICATION_JAR_LOCATION}  spark-worker-1:${SPARK_APPLICATION_JAR_LOCATION}
docker cp spark/docker-spark-cluster-master/${SPARK_APPLICATION_JAR_LOCATION}  spark-worker-2:/${SPARK_APPLICATION_JAR_LOCATION}

docker exec spark-master sh -c "spark/bin/spark-submit --master spark://spark-master:7077 ${SPARK_APPLICATION_JAR_LOCATION}"

