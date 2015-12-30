#!/usr/bin/env bash
echo Please read and modify $0 to work before attempting to run it. Thanks.
echo P.S., you\'ll need curl in your path.
exit 100

# Where to find your elasticsearch's REST API. Leave off the trailing '/'
ES_REST_PREFIX='http://localhost:9200'
echo create test-1-shard
curl -XPUT "$ES_REST_PREFIX/test-1-shard" -d '{
    "settings" : {
        "number_of_shards" : 1,
        "number_of_replicas" : 0
    }
}'
echo
echo create test-5-shards
curl -XPUT "$ES_REST_PREFIX/test-5-shards" -d '{
    "settings" : {
        "number_of_shards" : 5,
        "number_of_replicas" : 0
    }
}'
echo
echo create test-10-shards
curl -XPUT "$ES_REST_PREFIX/test-10-shards" -d '{
    "settings" : {
        "number_of_shards" : 10,
        "number_of_replicas" : 0
    }
}'
echo
echo create test-20-shards
curl -XPUT "$ES_REST_PREFIX/test-20-shards" -d '{
    "settings" : {
        "number_of_shards" : 20,
        "number_of_replicas" : 0
    }
}'
echo
echo create test-mismatched-shards
curl -XPUT "$ES_REST_PREFIX/test-mismatched-shards" -d '{
    "settings" : {
        "number_of_shards" : 9,
        "number_of_replicas" : 0
    }
}'
echo
echo create test-only-exists-in-es
curl -XPUT "$ES_REST_PREFIX/test-only-exists-in-es" -d '{
    "settings" : {
        "number_of_shards" : 1,
        "number_of_replicas" : 0
    }
}'
echo
