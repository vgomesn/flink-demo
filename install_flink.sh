#!/usr/bin/env bash
set -euo pipefail

if [ -d "flink-2.0.0" ]; then
  echo "Flink 2.0.0 already present at $(pwd)/flink-2.0.0"
  exit 0
fi

echo "Downloading Flink 2.0.0..."
curl -sSL https://archive.apache.org/dist/flink/flink-2.0.0/flink-2.0.0-bin-scala_2.12.tgz -o flink-2.0.0.tgz

echo "Extracting..."
tar -xzf flink-2.0.0.tgz
rm -f flink-2.0.0.tgz

# Bind UI for Gitpod & set a couple of sensible defaults
CONF="flink-2.0.0/conf/flink-conf.yaml"
sed -i 's@^#\?rest.bind-address:.*@rest.bind-address: 0.0.0.0@g' "$CONF" || true
grep -q '^rest.bind-address' "$CONF" || echo 'rest.bind-address: 0.0.0.0' >> "$CONF"
grep -q '^parallelism.default' "$CONF" || echo 'parallelism.default: 2' >> "$CONF"
grep -q '^taskmanager.numberOfTaskSlots' "$CONF" || echo 'taskmanager.numberOfTaskSlots: 2' >> "$CONF"

echo "Flink installed at: $(pwd)/flink-2.0.0"
