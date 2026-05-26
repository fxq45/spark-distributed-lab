#!/bin/bash
# 运行 PageRank 程序
# 使用方法: ./scripts/run-pagerank.sh

echo "=========================================="
echo "  提交 PageRank 任务到 Spark 集群"
echo "=========================================="

docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 512m \
    --executor-memory 512m \
    --executor-cores 1 \
    /opt/spark-apps/pagerank.py \
    /opt/spark-data/web_graph.txt \
    10

echo ""
echo "PageRank 任务完成!"
