docker cp HBASE/hbase-site.xml hbase-docker:/hbase/conf/hbase-site.xml
docker cp HBASE/create_tables.sh hbase-docker:/create_tables.sh

docker exec hbase-docker sh -c "hbase shell create_tables.sh"
