# project1SABD

## Index

- [Description](#Description)
- [Prerequisites](#Prerequisites)
- [Running Locally](#Running-Locally)


## Description
The project core consists in one jar which contains code and all dependencies, which is executed on a docker containers cluster
collocated on the same docker network. The involved containers are the following: a set of containers running hadoop cluster ,
particularly 1 master container and 3 worker container,a single container executing NiFi, a single container executing HBase 
and 3 containers executing Spark cluster, involving 1 master and 2 workers.

## Prerequisites
In order to run locally you need the images of the containers used to run the application.The hadoop image is efferre/hadoop 

``` 
docker pull effeerre/hadoop

```  
the HBase image is harisekhon/hbase 
```
docker pull harisekhon/hbase

``` 


## Running Locally
The previous container can be started all at once by executing the "start-all.sh" script, which creates the network and start 
all the containers running them in the same network.In order to see if all containers are up and running we can access the 
respective WebUI: http://localhost:9870 to access hdfs UI, http://localhost:16010/master-status to access HBase web UI, 
http://spark-master:8080 to access spark master UI and finally http://localhost:9090/nifi to access NiFi Web UI (it may take a while
for NiFi WebUI to become accessible after the container starts).


 


