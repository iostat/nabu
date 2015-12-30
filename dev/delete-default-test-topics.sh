#!/usr/bin/env bash
echo Please read and modify $0 to work before attempting to run it. Thanks.
echo P.S., you\'ll need kafka-topics.sh in your path.
exit 100

# ZK server + chroot of your kafka instance. Leave off the trailing '/'
KAFKA_ZK_STRING='localhost:2181/kafka-prod'

echo delete test-1-shard
kafka-topics.sh --zookeeper $KAFKA_ZK_STRING --delete --topic nabu-test-1-shard
echo delete test-5-shards
kafka-topics.sh --zookeeper $KAFKA_ZK_STRING --delete --topic nabu-test-5-shards
echo delete test-10-shards
kafka-topics.sh --zookeeper $KAFKA_ZK_STRING --delete --topic nabu-test-10-shards
echo delete test-20-shards
kafka-topics.sh --zookeeper $KAFKA_ZK_STRING --delete --topic nabu-test-20-shards
echo delete test-mismatched-shards
kafka-topics.sh --zookeeper $KAFKA_ZK_STRING --delete --topic nabu-test-mismatched-shards
echo delete test-only-exists-in-kafka
kafka-topics.sh --zookeeper $KAFKA_ZK_STRING --delete --topic nabu-test-only-exists-in-kafka
