#!/bin/bash
docker network create --driver bridge hadoopnet
docker run -t -i -p 9864:9864 -d --network=hadoopnet --name=slave1 effeerre/hadoop
docker run -t -i -p 9863:9864 -d --network=hadoopnet --name=slave2 effeerre/hadoop
docker run -t -i -p 9862:9864 -d --network=hadoopnet --name=slave3 effeerre/hadoop
docker run -t -i -p 9870:9870 -d -p 54310:54310 --network=hadoopnet --name=master effeerre/hadoop

docker cp HDFS/core-site.xml master:/usr/local/hadoop/etc/hadoop/core-site.xml
docker cp HDFS/hdfs-site.xml master:/usr/local/hadoop/etc/hadoop/hdfs-site.xml

docker exec master /bin/bash -c "hdfs namenode -format -force; \$HADOOP_HOME/sbin/start-dfs.sh"



