docker run -t -i --network=hadoopnet --name nifi -p 9090:8080 -e NIFI_WEB_HTTP_PORT='9090' -d apache/nifi:latest

docker cp hdfs-site.xml nifi:/opt/nifi/nifi-current/conf/hdfs-site.xml
docker cp core-site.xml nifi:/opt/nifi/nifi-current/conf/core-site.xml

docker exec -u root -t -i nifi /bin/bash

#docker cp data/prj1_dataset nifi:/home/data



#sudo sh ./start-nifi.sh

