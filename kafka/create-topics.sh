#!/bin/bash
# Creates the Kafka topics listed in topics.txt (name partitions). Idempotent; run by `kafka-init`.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka:9092}"
TOPICS=/opt/kafka/bin/kafka-topics.sh

for _ in $(seq 1 60); do
  $TOPICS --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1 && break
  sleep 2
done

grep -vE '^\s*(#|$)' /kafka/topics.txt | while read -r name partitions _description; do
  $TOPICS --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$name" --partitions "$partitions"
done
$TOPICS --bootstrap-server "$BOOTSTRAP" --list
