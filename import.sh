#!/bin/bash

JSON=$(cat <<EOF
{
    "type":"mysql",
    "mysql":{
        "index":"elasticSearchIndexName",
        "type":"elasticSearchTypeName",
        "hostname":"databaseHostname:3306",
        "database":"databaseName",
        "username":"databaseUser",
        "password":"databasePassword",
        "uniqueIdField":"PrimaryKeyFieldName",
        "query":"select distinct id AS PrimaryKeyFieldName, field1, field2, field3  FROM MyTable t where field4 != NULL",
        "deleteOldEntries":"false",
        "interval":"60000"
    }
}
EOF
)

curl -XDELETE 127.0.0.1:9200/_river/river-mysql
echo
curl -XPUT 127.0.0.1:9200/_river/river-mysql/_meta -d "$JSON"
echo
