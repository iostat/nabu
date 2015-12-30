#!/usr/bin/env bash
echo Please read and modify $0 to work before attempting to run it. Thanks.
echo P.S., you\'ll need curl in your path.
exit 100

# Where to find your elasticsearch's REST API. Leave off the trailing '/'
ES_REST_PREFIX='http://localhost:9200'

echo delete test-1-shard
curl -XDELETE "$ES_REST_PREFIX/test-1-shard"
echo
echo delete test-5-shards
curl -XDELETE "$ES_REST_PREFIX/test-5-shards"
echo
echo delete test-10-shards
curl -XDELETE "$ES_REST_PREFIX/test-10-shards"
echo
echo delete test-20-shards
curl -XDELETE "$ES_REST_PREFIX/test-20-shards"
echo
echo delete test-20-shards
curl -XDELETE "$ES_REST_PREFIX/test-mismatched-shards"
echo
echo delete test-only-exists-in-es
curl -XDELETE "$ES_REST_PREFIX/test-only-exists-in-es"
echo
