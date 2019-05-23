docker cp hbase-site.xml hbase-docker:/hbase/conf/hbase-site.xml
docker cp create_tables.sh hbase-docker:/create_tables.sh

docker exec hbase-docker sh -c "hbase shell create_tables.sh"
