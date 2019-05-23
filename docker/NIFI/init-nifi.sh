docker cp hdfs-site.xml nifi:/opt/nifi/nifi-current/conf/hdfs-site.xml
docker cp core-site.xml nifi:/opt/nifi/nifi-current/conf/core-site.xml
docker cp hbase-site.xml nifi:/opt/nifi/nifi-current/conf/hbase-site.xml


docker cp data/prj1_dataset nifi:/home/data

