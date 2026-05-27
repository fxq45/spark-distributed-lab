# Spark 分布式计算实验

> 《典型分布式（云）技术及系统》课程实践考核项目

## 项目概述

本项目基于 Apache Spark 框架，使用 Docker 部署分布式集群环境，实现了两个经典的分布式计算程序：

1. **PageRank 算法** — 基于图的迭代计算，利用 Spark RDD 分布式计算网页排名
2. **TF-IDF 文本相似度分析** — 利用 Spark MLlib 进行分布式文本特征提取与相似度计算

## 系统架构

```
┌─────────────────────────────────────────────────┐
│                  Spark Cluster                    │
│                                                  │
│  ┌──────────────┐                               │
│  │ Spark Master │  (调度 & 资源管理)              │
│  │  :8080 UI    │                               │
│  └──────┬───────┘                               │
│         │                                        │
│    ┌────┴────┐                                  │
│    │         │                                  │
│  ┌─┴──────┐ ┌─┴──────┐                         │
│  │Worker-1│ │Worker-2│  (任务执行)               │
│  │ 1G/2C  │ │ 1G/2C  │                         │
│  └────────┘ └────────┘                          │
│                                                  │
└─────────────────────────────────────────────────┘
         Docker Network (spark-network)
```

## 环境要求

- Docker >= 20.10（内置 Compose 插件，无需单独安装 docker-compose）
- 至少 4GB 可用内存
- **无需访问 Docker Hub**：镜像从清华镜像站下载 Spark 本地构建，国内网络友好

## 快速开始

### 1. 启动集群

```bash
./scripts/start-cluster.sh
```

启动后可访问：
- Spark Master UI: http://localhost:8080
- Spark Worker 1 UI: http://localhost:8081
- Spark Worker 2 UI: http://localhost:8082

### 2. 运行 PageRank

```bash
./scripts/run-pagerank.sh
```

### 3. 运行 TF-IDF 文本相似度分析

```bash
./scripts/run-tfidf.sh
```

### 4. 停止集群

```bash
./scripts/stop-cluster.sh
```

## 项目结构

```
spark-distributed-lab/
├── docker/
│   └── docker-compose.yml      # Docker Compose 集群配置
├── src/
│   ├── pagerank.py             # PageRank 分布式实现
│   └── tfidf_similarity.py     # TF-IDF 文本相似度分析
├── data/
│   ├── web_graph.txt           # PageRank 测试数据（网页链接图）
│   └── documents/              # TF-IDF 测试文档集
├── scripts/
│   ├── start-cluster.sh        # 启动集群
│   ├── stop-cluster.sh         # 停止集群
│   ├── run-pagerank.sh         # 运行 PageRank
│   └── run-tfidf.sh            # 运行 TF-IDF
├── docs/
│   ├── deploy-report.md        # 部署配置测试报告
│   └── experiment-report.md    # 实验报告
└── README.md
```

## 技术栈

| 组件 | 版本 | 用途 |
|------|------|------|
| Apache Spark | 3.5.8 | 分布式计算框架 |
| PySpark | 3.5.8 | Python API |
| Docker | 20.10+ | 容器化部署 |
| Ubuntu 22.04 + OpenJDK 17 | - | 自构建 Spark 镜像基础 |

## 算法说明

### PageRank

PageRank 是 Google 搜索引擎使用的核心算法之一，用于衡量网页的重要性。

**核心公式：**
```
PR(A) = (1-d)/N + d × Σ(PR(Ti)/C(Ti))
```
- d = 0.85（阻尼系数）
- N = 节点总数
- Ti = 指向 A 的页面
- C(Ti) = Ti 的出链数

**分布式实现要点：**
- 使用 RDD 存储图的邻接表
- 每次迭代通过 `flatMap` + `reduceByKey` 分布式计算贡献值
- 利用 `cache()` 缓存不变的链接数据

### TF-IDF 文本相似度

TF-IDF（词频-逆文档频率）是信息检索中的经典算法。

**核心公式：**
```
TF-IDF(t,d) = TF(t,d) × IDF(t)
TF(t,d) = 词t在文档d中出现的次数 / 文档d的总词数
IDF(t) = log(文档总数 / 包含词t的文档数)
```

**分布式实现要点：**
- 使用 Spark MLlib 的 Pipeline 进行分布式特征提取
- HashingTF 进行分布式词频统计
- IDF 模型分布式计算逆文档频率
- 余弦相似度计算文档间的相似程度
