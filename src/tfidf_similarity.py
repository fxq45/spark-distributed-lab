"""
TF-IDF 文本相似度分析系统 - Spark 分布式实现

利用 Spark 计算文档集合的 TF-IDF 权重，并通过余弦相似度找出最相似的文档对。
这是一个典型的分布式计算任务，涉及：
  1. 分布式文本预处理
  2. 分布式 TF-IDF 计算
  3. 分布式余弦相似度计算

输入：文本文件目录（每个文件为一个文档）
输出：文档相似度矩阵 + Top-K 相似文档对
"""

from pyspark.sql import SparkSession
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover
from pyspark.sql.functions import col, udf, explode, split, lower, regexp_replace
from pyspark.sql.types import FloatType, ArrayType, StringType
from pyspark.ml.linalg import SparseVector, DenseVector
import sys
import time
import math


def cosine_similarity(v1, v2):
    """计算两个向量的余弦相似度"""
    if v1 is None or v2 is None:
        return 0.0

    # 转换为 dense 进行计算
    if isinstance(v1, SparseVector):
        v1 = v1.toArray()
    if isinstance(v2, SparseVector):
        v2 = v2.toArray()

    dot_product = sum(a * b for a, b in zip(v1, v2))
    norm1 = math.sqrt(sum(a * a for a in v1))
    norm2 = math.sqrt(sum(b * b for b in v2))

    if norm1 == 0 or norm2 == 0:
        return 0.0
    return float(dot_product / (norm1 * norm2))


def main():
    if len(sys.argv) < 2:
        print("Usage: tfidf_similarity.py <input_dir> [num_features] [top_k]")
        print("  input_dir: 文档目录路径")
        print("  num_features: 特征维度（默认1024）")
        print("  top_k: 输出前K个相似对（默认10）")
        sys.exit(1)

    input_dir = sys.argv[1]
    num_features = int(sys.argv[2]) if len(sys.argv) > 2 else 1024
    top_k = int(sys.argv[3]) if len(sys.argv) > 3 else 10

    # 创建 SparkSession
    spark = SparkSession.builder \
        .appName("TF-IDF-Similarity-Analysis") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("  TF-IDF 文本相似度分析系统")
    print("=" * 60)
    print(f"  输入目录: {input_dir}")
    print(f"  特征维度: {num_features}")
    print(f"  输出 Top-K: {top_k}")
    print(f"  Spark Master: {spark.sparkContext.master}")
    print("=" * 60)

    start_time = time.time()

    # 1. 读取文档集合
    # 使用 wholeTextFiles 读取整个目录，每个文件作为一个文档
    raw_docs = spark.sparkContext.wholeTextFiles(input_dir)
    docs_list = raw_docs.collect()

    print(f"\n  读取到 {len(docs_list)} 个文档")

    # 转换为 DataFrame
    doc_data = [(i, docs_list[i][0].split("/")[-1], docs_list[i][1])
                for i in range(len(docs_list))]
    df = spark.createDataFrame(doc_data, ["doc_id", "filename", "content"])

    # 2. 文本预处理
    print("  正在进行文本预处理...")

    # 转小写并去除标点
    df = df.withColumn("cleaned",
                       regexp_replace(lower(col("content")), "[^a-zA-Z\\s]", " "))

    # 分词
    tokenizer = Tokenizer(inputCol="cleaned", outputCol="words")
    df = tokenizer.transform(df)

    # 去除停用词
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    df = remover.transform(df)

    # 3. 计算 TF-IDF
    print("  正在计算 TF-IDF 权重...")

    # Term Frequency
    hashing_tf = HashingTF(inputCol="filtered_words", outputCol="raw_features",
                           numFeatures=num_features)
    tf_df = hashing_tf.transform(df)

    # Inverse Document Frequency
    idf = IDF(inputCol="raw_features", outputCol="tfidf_features")
    idf_model = idf.fit(tf_df)
    tfidf_df = idf_model.transform(tf_df)

    # 4. 计算文档间余弦相似度
    print("  正在计算文档相似度矩阵...")

    # 收集所有文档的 TF-IDF 向量
    doc_vectors = tfidf_df.select("doc_id", "filename", "tfidf_features").collect()
    num_docs = len(doc_vectors)

    # 计算两两相似度
    similarities = []
    for i in range(num_docs):
        for j in range(i + 1, num_docs):
            sim = cosine_similarity(
                doc_vectors[i]["tfidf_features"],
                doc_vectors[j]["tfidf_features"]
            )
            similarities.append((
                doc_vectors[i]["filename"],
                doc_vectors[j]["filename"],
                sim
            ))

    # 按相似度降序排序
    similarities.sort(key=lambda x: x[2], reverse=True)

    elapsed_time = time.time() - start_time

    # 5. 输出结果
    print(f"\n  计算完成! 耗时: {elapsed_time:.2f} 秒")
    print(f"\n  {'=' * 60}")
    print(f"  Top {top_k} 最相似文档对:")
    print(f"  {'=' * 60}")
    print(f"  {'排名':<5}{'文档A':<20}{'文档B':<20}{'相似度':<10}")
    print(f"  {'-' * 60}")

    for i, (doc_a, doc_b, sim) in enumerate(similarities[:top_k]):
        print(f"  {i + 1:<5}{doc_a:<20}{doc_b:<20}{sim:.6f}")

    print(f"  {'=' * 60}")

    # 输出统计信息
    if similarities:
        avg_sim = sum(s[2] for s in similarities) / len(similarities)
        max_sim = similarities[0][2]
        min_sim = similarities[-1][2]
        print(f"\n  统计信息:")
        print(f"    文档总数: {num_docs}")
        print(f"    文档对总数: {len(similarities)}")
        print(f"    平均相似度: {avg_sim:.6f}")
        print(f"    最高相似度: {max_sim:.6f}")
        print(f"    最低相似度: {min_sim:.6f}")

    # 6. 输出每个文档的关键词（TF-IDF 最高的词）
    print(f"\n  {'=' * 60}")
    print(f"  各文档 Top 5 关键词 (按 TF-IDF 权重):")
    print(f"  {'=' * 60}")

    # 获取词汇对应关系
    for row in tfidf_df.select("filename", "filtered_words", "tfidf_features").collect():
        filename = row["filename"]
        words = row["filtered_words"]
        features = row["tfidf_features"]

        if isinstance(features, SparseVector):
            # 获取非零元素的索引和值
            indices = features.indices
            values = features.values
            # 按值降序排序
            word_scores = sorted(zip(indices, values), key=lambda x: x[1], reverse=True)
            print(f"\n  {filename}:")

            # 找到对应的词（通过 hash 匹配）
            word_hash_map = {}
            for word in words:
                h = hash(word) % num_features
                if h < 0:
                    h += num_features
                word_hash_map[h] = word

            shown = 0
            for idx, score in word_scores:
                if idx in word_hash_map and shown < 5:
                    print(f"    - {word_hash_map[idx]}: {score:.4f}")
                    shown += 1

    spark.stop()
    print(f"\n  {'=' * 60}")
    print("  程序结束。")


if __name__ == "__main__":
    main()
