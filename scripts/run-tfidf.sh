#!/bin/bash
# 运行 TF-IDF 文本相似度分析
# 使用方法: ./scripts/run-tfidf.sh

echo "=========================================="
echo "  提交 TF-IDF 相似度分析任务到 Spark 集群"
echo "=========================================="

docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 512m \
    --executor-memory 512m \
    --executor-cores 1 \
    /opt/spark-apps/tfidf_similarity.py \
    /opt/spark-data/documents \
    1024 \
    10

echo ""
echo "TF-IDF 分析任务完成!"
