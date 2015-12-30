#!/usr/bin/env bash
echo Please read and modify $0 to work before attempting to run it. Thanks.
echo P.S., you\'ll need kafka-topics.sh in your path.
exit 100

# ZK server + chroot of your kafka instance. Leave off the trailing '/'
KAFKA_ZK_STRING='localhost:2181/kafka-prod'

echo create test-1-shard
kafka-topics.sh --zookeeper $KAFKA_ZK_STRING --create --topic nabu-test-1-shard --partitions 1 --replication-factor 1
echo create test-5-shards
kafka-topics.sh --zookeeper $KAFKA_ZK_STRING --create --topic nabu-test-5-shards --partitions 5 --replication-factor 1
echo create test-10-shards
kafka-topics.sh --zookeeper $KAFKA_ZK_STRING --create --topic nabu-test-10-shards --partitions 10 --replication-factor 1
echo create test-20-shards
kafka-topics.sh --zookeeper $KAFKA_ZK_STRING --create --topic nabu-test-20-shards --partitions 20 --replication-factor 1
echo create test-mismatched-shards
kafka-topics.sh --zookeeper $KAFKA_ZK_STRING --create --topic nabu-test-mismatched-shards --partitions 4 --replication-factor 1
echo create test-only-exists-in-kafka
kafka-topics.sh --zookeeper $KAFKA_ZK_STRING --create --topic nabu-test-only-exists-in-kafka --partitions 1 --replication-factor 1
