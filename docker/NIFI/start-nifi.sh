docker run -t -i --network=hadoopnet --name nifi -p 9090:8080 -e NIFI_WEB_HTTP_PORT='9090' -d apache/nifi:latest

docker cp NIFI/hdfs-site.xml nifi:/opt/nifi/nifi-current/conf/hdfs-site.xml
docker cp NIFI/core-site.xml nifi:/opt/nifi/nifi-current/conf/core-site.xml
docker cp NIFI/hbase-site.xml nifi:/opt/nifi/nifi-current/conf/hbase-site.xml


docker cp NIFI/data/prj1_dataset nifi:/home/data




