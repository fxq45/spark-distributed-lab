#!/bin/bash
# 启动 Spark 集群
# 使用方法: ./scripts/start-cluster.sh

echo "=========================================="
echo "  启动 Spark 分布式集群"
echo "=========================================="

cd "$(dirname "$0")/../docker"

# 首次运行需要构建镜像（从清华镜像站下载 Spark，无需访问 Docker Hub）
echo ""
echo "检查/构建 Spark 镜像（首次约需 3-5 分钟）..."
docker compose build

# 启动集群
echo ""
echo "启动集群容器..."
docker compose up -d

echo ""
echo "等待集群启动..."
sleep 15

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
