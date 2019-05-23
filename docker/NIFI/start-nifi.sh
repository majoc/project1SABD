docker run -t -i --network=hadoopnet --name nifi -p 9090:8080 -e NIFI_WEB_HTTP_PORT='9090' -d apache/nifi:latest



