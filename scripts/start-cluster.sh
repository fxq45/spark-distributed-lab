#!/bin/bash
# 启动 Spark 集群
# 使用方法: ./scripts/start-cluster.sh

echo "=========================================="
echo "  启动 Spark 分布式集群"
echo "=========================================="

cd "$(dirname "$0")/../docker"

# 启动集群
docker compose up -d

echo ""
echo "等待集群启动..."
sleep 10

# 检查状态
echo ""
echo "集群状态:"
docker compose ps

echo ""
echo "=========================================="
echo "  集群已启动!"
echo "  Spark Master Web UI: http://localhost:8080"
echo "  Spark Worker 1 UI:   http://localhost:8081"
echo "  Spark Worker 2 UI:   http://localhost:8082"
echo "=========================================="
