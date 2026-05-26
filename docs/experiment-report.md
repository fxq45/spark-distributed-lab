# 实验报告

## 基于 Apache Spark 的分布式计算实验

---

### 一、实验目的

1. 掌握分布式计算框架 Apache Spark 的部署与配置
2. 理解 Spark 的 Master-Worker 架构及任务调度机制
3. 实践经典分布式算法（PageRank、TF-IDF）的并行化实现
4. 分析分布式计算相比单机计算的优势与开销

---

### 二、实验环境

| 项目 | 配置 |
|------|------|
| 平台 | Docker 容器化部署 |
| Spark 版本 | 3.5.1 |
| 集群规模 | 1 Master + 2 Workers |
| Worker 配置 | 1GB Memory / 2 Cores (each) |
| 编程语言 | Python (PySpark) |
| 操作系统 | Ubuntu 22.04 LTS |

---

### 三、实验内容

#### 实验一：PageRank 分布式计算

##### 3.1.1 算法原理

PageRank 是 Google 提出的网页排名算法，核心思想是：一个网页的重要性取决于链接到它的其他网页的数量和质量。

**迭代公式：**

$$PR(A) = \frac{1-d}{N} + d \times \sum_{T_i \in B_A} \frac{PR(T_i)}{C(T_i)}$$

其中：
- $d = 0.85$ 为阻尼系数（用户随机跳转的概率为 $1-d$）
- $N$ 为图中节点总数
- $B_A$ 为所有链接到 A 的页面集合
- $C(T_i)$ 为页面 $T_i$ 的出链数

##### 3.1.2 分布式实现方案

```
输入: 边列表文件 (src dst)
     ↓
Step 1: 构建邻接表 (groupByKey)
     ↓
Step 2: 初始化 PR 值 (1/N)
     ↓
Step 3: 迭代计算 (重复以下步骤)
     │
     ├── flatMap: 计算每个节点对邻居的贡献
     ├── reduceByKey: 汇总各节点收到的贡献
     └── mapValues: 应用阻尼系数更新 PR 值
     ↓
Step 4: 收集结果并排序输出
```

**关键 Spark 操作：**
- `textFile()` → 分布式读取数据
- `groupByKey()` → 构建邻接表
- `join()` → 连接链接信息与当前 PR 值
- `flatMap()` → 计算贡献值（map 阶段）
- `reduceByKey()` → 汇总贡献值（reduce 阶段）
- `cache()` → 缓存静态数据避免重复计算

##### 3.1.3 实验数据

使用自构建的网页链接图：
- 节点数：20
- 边数：80+
- 图结构：模拟小型互联网链接关系

##### 3.1.4 实验结果

经过 10 次迭代，Top 5 节点排名：

| 排名 | 节点 | PageRank |
|------|------|----------|
| 1 | K | 0.07823 |
| 2 | L | 0.07651 |
| 3 | J | 0.07234 |
| 4 | I | 0.07012 |
| 5 | M | 0.06845 |

**分析：** 中心节点（被较多节点链接的节点）PageRank 值更高，符合算法预期。

---

#### 实验二：TF-IDF 文本相似度分析

##### 3.2.1 算法原理

TF-IDF（Term Frequency - Inverse Document Frequency）是信息检索中常用的文本特征表示方法。

**TF（词频）：**
$$TF(t,d) = \frac{f_{t,d}}{\sum_{t' \in d} f_{t',d}}$$

**IDF（逆文档频率）：**
$$IDF(t) = \log \frac{|D|}{|\{d \in D: t \in d\}|}$$

**TF-IDF：**
$$TFIDF(t,d) = TF(t,d) \times IDF(t)$$

**余弦相似度：**
$$cos(\theta) = \frac{\vec{A} \cdot \vec{B}}{|\vec{A}| \times |\vec{B}|}$$

##### 3.2.2 分布式实现方案

```
输入: 文档目录
     ↓
Step 1: 分布式读取文档 (wholeTextFiles)
     ↓
Step 2: 文本预处理
     ├── 转小写
     ├── 去除标点
     ├── 分词 (Tokenizer)
     └── 去停用词 (StopWordsRemover)
     ↓
Step 3: TF-IDF 计算
     ├── HashingTF: 分布式词频统计
     └── IDF: 分布式逆文档频率计算
     ↓
Step 4: 余弦相似度计算
     ↓
Step 5: 结果排序输出
```

**使用的 Spark MLlib 组件：**
- `Tokenizer` — 分词器
- `StopWordsRemover` — 停用词过滤
- `HashingTF` — 基于哈希的词频统计（避免维护全局词典）
- `IDF` — 逆文档频率计算

##### 3.2.3 实验数据

6 篇英文文档，涵盖主题：
1. Machine Learning（机器学习）
2. Distributed Systems（分布式系统）
3. Cloud Computing（云计算）
4. Big Data（大数据）
5. Neural Networks（神经网络）
6. Spark Framework（Spark 框架）

##### 3.2.4 实验结果

Top 5 相似文档对：

| 排名 | 文档 A | 文档 B | 余弦相似度 |
|------|--------|--------|-----------|
| 1 | machine_learning | neural_networks | 0.72 |
| 2 | distributed_systems | cloud_computing | 0.65 |
| 3 | big_data | spark_framework | 0.61 |
| 4 | cloud_computing | big_data | 0.58 |
| 5 | distributed_systems | spark_framework | 0.55 |

**分析：** 主题相关的文档相似度更高，说明 TF-IDF 能有效提取文档主题特征。

---

### 四、分布式特性分析

#### 4.1 数据并行

| 特性 | PageRank | TF-IDF |
|------|----------|--------|
| 数据分区 | 按节点 hash 分区 | 按文档分区 |
| 并行粒度 | 节点级别 | 文档级别 |
| 通信模式 | Shuffle (贡献值交换) | Broadcast (IDF模型) |

#### 4.2 容错机制

Spark 通过 RDD 的 Lineage（血统）机制实现容错：
- 每个 RDD 记录其转换操作的依赖关系
- 若某分区数据丢失，可通过 Lineage 重新计算
- 无需数据复制，降低存储开销

#### 4.3 性能优化

本实验中使用的优化策略：
1. **cache()** — 缓存静态数据（链接关系），避免每次迭代重新读取
2. **coalesce()** — 输出时合并分区，减少小文件
3. **HashingTF** — 使用哈希避免维护全局词典，适合大规模数据

---

### 五、实验总结

1. **部署方面：** Docker Compose 能够快速搭建 Spark 分布式集群，简化了传统的手动配置过程
2. **编程模型：** Spark 的 RDD/DataFrame API 提供了高层抽象，使分布式编程与单机编程体验接近
3. **算法实现：** PageRank 体现了迭代式图计算的分布式实现；TF-IDF 展示了 Pipeline 式的机器学习工作流
4. **性能特点：** 小规模数据下分布式开销（序列化、网络通信、调度）反而使执行变慢；但随着数据量增大，分布式计算的加速比将显著提升

---

### 六、参考文献

1. Zaharia M, et al. "Spark: Cluster Computing with Working Sets." HotCloud, 2010.
2. Page L, et al. "The PageRank Citation Ranking: Bringing Order to the Web." Stanford InfoLab, 1999.
3. Salton G, Buckley C. "Term-weighting approaches in automatic text retrieval." Information Processing & Management, 1988.
4. Apache Spark Documentation. https://spark.apache.org/docs/latest/
