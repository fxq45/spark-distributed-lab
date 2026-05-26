"""
PageRank 算法 - Spark 分布式实现

基于 Google 的 PageRank 算法，利用 Spark RDD 进行分布式迭代计算。
输入：图的邻接表（边列表文件）
输出：各节点的 PageRank 值

算法原理：
  PR(A) = (1-d)/N + d * Σ(PR(Ti)/C(Ti))
  其中 d=0.85 为阻尼系数，N 为节点总数，Ti 为指向 A 的页面，C(Ti) 为 Ti 的出链数
"""

from pyspark.sql import SparkSession
import sys
import time


def parse_edge(line):
    """解析边：格式为 'src dst' 或 'src\tdst'"""
    parts = line.strip().split()
    if len(parts) >= 2:
        return (parts[0], parts[1])
    return None


def compute_contributions(neighbors, rank):
    """计算节点对其邻居的贡献值"""
    num_neighbors = len(neighbors)
    for neighbor in neighbors:
        yield (neighbor, rank / num_neighbors)


def main():
    if len(sys.argv) < 2:
        print("Usage: pagerank.py <input_file> [iterations] [output_file]")
        print("  input_file: 边列表文件路径")
        print("  iterations: 迭代次数（默认10）")
        print("  output_file: 输出文件路径（可选）")
        sys.exit(1)

    input_file = sys.argv[1]
    iterations = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    output_file = sys.argv[3] if len(sys.argv) > 3 else None

    # 创建 SparkSession
    spark = SparkSession.builder \
        .appName("PageRank-Distributed") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    print("=" * 60)
    print("  PageRank 分布式计算")
    print("=" * 60)
    print(f"  输入文件: {input_file}")
    print(f"  迭代次数: {iterations}")
    print(f"  Spark Master: {sc.master}")
    print("=" * 60)

    start_time = time.time()

    # 1. 读取边列表并构建邻接表
    lines = sc.textFile(input_file)

    # 过滤注释行和空行
    edges = lines.filter(lambda line: line.strip() and not line.startswith('#')) \
                 .map(parse_edge) \
                 .filter(lambda x: x is not None)

    # 构建邻接表: (node, [neighbors])
    links = edges.groupByKey().mapValues(list).cache()

    # 获取节点总数
    num_nodes = links.count()
    print(f"\n  图信息: {num_nodes} 个节点")
    print(f"  总边数: {edges.count()}")

    # 2. 初始化 PageRank 值（均匀分布）
    ranks = links.mapValues(lambda _: 1.0 / num_nodes)

    # 3. 迭代计算 PageRank
    damping_factor = 0.85
    print(f"\n  阻尼系数: {damping_factor}")
    print(f"\n  开始迭代计算...")

    for i in range(iterations):
        # 计算每个节点的贡献
        contributions = links.join(ranks).flatMap(
            lambda node: compute_contributions(node[1][0], node[1][1])
        )

        # 更新 PageRank 值
        ranks = contributions.reduceByKey(lambda x, y: x + y) \
            .mapValues(lambda rank: (1 - damping_factor) / num_nodes + damping_factor * rank)

        if (i + 1) % 5 == 0 or i == 0:
            print(f"    第 {i + 1}/{iterations} 次迭代完成")

    # 4. 收集结果并排序
    results = ranks.collect()
    results.sort(key=lambda x: x[1], reverse=True)

    elapsed_time = time.time() - start_time

    # 5. 输出结果
    print(f"\n  计算完成! 耗时: {elapsed_time:.2f} 秒")
    print(f"\n  {'=' * 50}")
    print(f"  Top 20 PageRank 结果:")
    print(f"  {'=' * 50}")
    print(f"  {'排名':<6}{'节点':<15}{'PageRank':<15}")
    print(f"  {'-' * 50}")

    for i, (node, rank) in enumerate(results[:20]):
        print(f"  {i + 1:<6}{node:<15}{rank:.8f}")

    print(f"  {'=' * 50}")
    print(f"  总节点数: {num_nodes}")
    print(f"  PageRank 总和: {sum(r for _, r in results):.6f}")

    # 6. 保存结果到文件
    if output_file:
        output_rdd = sc.parallelize(results)
        output_rdd.map(lambda x: f"{x[0]}\t{x[1]:.10f}") \
                  .coalesce(1) \
                  .saveAsTextFile(output_file)
        print(f"\n  结果已保存到: {output_file}")

    spark.stop()
    print("\n  程序结束。")


if __name__ == "__main__":
    main()
