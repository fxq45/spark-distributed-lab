#!/bin/bash
# 停止 Spark 集群
# 使用方法: ./scripts/stop-cluster.sh

echo "停止 Spark 集群..."
cd "$(dirname "$0")/../docker"
docker compose down
echo "集群已停止。"
