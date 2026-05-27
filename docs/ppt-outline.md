# PPT 大纲 —— 基于 Apache Spark 的分布式计算实验

> 给 Kimi 的完整指令：请根据以下大纲生成一份 PPT，共 15-18 页。风格简洁专业，配色以深蓝+白色为主，科技感。每页内容不要太多文字，多用图表和关键词。10分钟演讲节奏。

---

## PPT 验收标准（发给 Kimi 的要求）

1. **总页数**：15-18 页（不含封面和结束页）
2. **每页文字**：标题 + 不超过 5 个要点，每个要点不超过 20 字
3. **必须包含的图**：系统架构图（Master-Worker）、PageRank 算法流程图、TF-IDF 计算流程图、实验结果表格
4. **配色**：科技蓝（#1E3A5F）+ 白色背景，图表用蓝橙对比色
5. **字体**：标题用粗体 28-32pt，正文 18-20pt，代码用等宽字体 16pt
6. **动画**：不要花哨动画，简单的逐条出现即可
7. **演讲时间**：控制在 10 分钟内（每页约 30-40 秒）

---

## 第 1 页：封面

- **标题**：基于 Apache Spark 的分布式计算实验
- **副标题**：典型分布式（云）技术及系统 · 课程实践
- **信息**：姓名、学号、日期
- **背景**：放一个 Spark Logo + 简单的集群网络示意图

---

## 第 2 页：目录

- 一、选题背景与目标
- 二、Apache Spark 简介
- 三、系统架构设计
- 四、环境部署
- 五、算法一：PageRank 分布式计算
- 六、算法二：TF-IDF 文本相似度分析
- 七、实验结果与分析
- 八、总结与展望

---

## 第 3 页：选题背景与目标

**要点：**
- 大数据时代，单机计算能力有限
- 分布式计算：多节点协同处理海量数据
- 本次实验目标：
  - 部署 Spark 分布式集群
  - 实现经典分布式算法（PageRank + TF-IDF）
  - 验证分布式计算的并行处理能力

**配图**：一张单机 vs 集群的对比图（1人搬箱子 vs 3人搬箱子的简图）

---

## 第 4 页：Apache Spark 简介

**要点：**
- Apache Spark：统一的大规模数据处理引擎（2014 年开源）
- 比 Hadoop MapReduce 快 10-100 倍（内存计算）
- 支持多种计算模式：批处理、流处理、机器学习、图计算
- 核心抽象：RDD（弹性分布式数据集）
- 编程语言支持：Python、Java、Scala、R

**配图**：Spark 生态系统图（Spark Core → Spark SQL / MLlib / GraphX / Streaming）

---

## 第 5 页：Spark 核心概念

**要点（用图+关键词）：**
- **RDD**：不可变、可分区、可并行操作的数据集合
- **Transformation**：惰性操作（map、filter、groupByKey）→ 不立即执行
- **Action**：触发实际计算（collect、count、saveAsTextFile）
- **DAG 调度**：将任务转化为有向无环图，优化执行顺序
- **Shuffle**：跨节点的数据交换（最耗性能的操作）

**配图**：一个简单的 RDD 转换链示意图（textFile → map → filter → reduceByKey → collect）

---

## 第 6 页：系统架构设计

**要点：**
- Master-Worker 架构
- 1 个 Master 节点：任务调度 + 资源管理
- 2 个 Worker 节点：任务执行 + 数据处理
- Docker 容器化部署，各节点独立 IP，通过虚拟网络通信

**配图（核心页，重点画好）：**
```
┌─────────────────────────────────────┐
│           Spark Cluster             │
│                                     │
│    ┌──────────────────┐             │
│    │   Spark Master   │             │
│    │  调度 & 资源管理  │             │
│    │  172.18.0.2:7077 │             │
│    └────────┬─────────┘             │
│        ┌────┴────┐                  │
│   ┌────┴───┐ ┌───┴────┐            │
│   │Worker-1│ │Worker-2│            │
│   │2C / 1G │ │2C / 1G │            │
│   │.0.3    │ │.0.4    │            │
│   └────────┘ └────────┘            │
│                                     │
│   Docker Network (172.18.0.0/16)   │
└─────────────────────────────────────┘
```

---

## 第 7 页：环境部署方案

**要点：**
- 使用 Docker + Docker Compose 一键部署
- 基础镜像：Ubuntu 22.04 + OpenJDK 17 + Spark 3.5.8
- 自构建镜像，不依赖外部仓库
- 一条命令启动集群：`./scripts/start-cluster.sh`

**代码片段（小字展示 docker-compose.yml 核心部分）：**
```yaml
services:
  spark-master:
    image: spark-local:3.5.8
    command: start-master.sh
  spark-worker-1:
    image: spark-local:3.5.8
    command: start-worker.sh spark://spark-master:7077
  spark-worker-2:
    image: spark-local:3.5.8
    command: start-worker.sh spark://spark-master:7077
```

---

## 第 8 页：部署验证（放截图）

**要点：**
- Spark Master Web UI (localhost:8080)
- 2 个 Worker 注册成功，状态 ALIVE
- 集群资源：4 Cores / 2 GB Memory

**配图**：放你截的第一张空闲状态的 Spark Web UI 截图

---

## 第 9 页：算法一 — PageRank 原理

**要点：**
- Google 1998 年提出的网页排名算法
- 核心思想：被更多重要页面链接的页面更重要
- 迭代公式：PR(A) = (1-d)/N + d × Σ(PR(Ti)/C(Ti))
  - d = 0.85（阻尼系数，用户继续点击链接的概率）
  - N = 节点总数
  - Ti = 指向 A 的页面
  - C(Ti) = Ti 的出链数

**配图**：一个 4-5 个节点的小图，标注箭头方向和 PR 值，直观展示"重要的节点指向你，你就更重要"

---

## 第 10 页：PageRank 分布式实现

**要点（流程图）：**
```
输入边列表 → textFile (分布式读取)
     ↓
构建邻接表 → groupByKey (按节点分组)
     ↓
初始化 PR 值 → 每个节点 1/N
     ↓
迭代 10 次：
  ├── join: 连接链接信息与 PR 值
  ├── flatMap: 计算每个节点的贡献值（并行）
  ├── reduceByKey: 汇总贡献值（Shuffle）
  └── mapValues: 应用阻尼系数更新 PR
     ↓
输出 Top-K 排名
```

**关键点强调：**
- `flatMap` + `reduceByKey` = 经典的 MapReduce 模式
- `cache()` 缓存静态链接数据，避免重复读取

---

## 第 11 页：PageRank 实验结果

**要点：**
- 测试数据：20 个节点，84 条边
- 迭代次数：10
- 耗时：8.95 秒
- PR 总和 = 1.000000（验证正确性）

**表格（Top 10）：**

| 排名 | 节点 | PageRank |
|------|------|----------|
| 1 | G | 0.06781 |
| 2 | I | 0.06373 |
| 3 | D | 0.06157 |
| 4 | E | 0.05980 |
| 5 | R | 0.05642 |
| ... | ... | ... |

**配图**：放 PageRank 终端输出截图

---

## 第 12 页：算法二 — TF-IDF 原理

**要点：**
- TF-IDF = 词频 × 逆文档频率（信息检索经典算法）
- TF(t,d) = 词 t 在文档 d 中的出现次数 / 文档 d 总词数
- IDF(t) = log(文档总数 / 包含词 t 的文档数)
- 一个词在某文档中出现频率高 + 在其他文档中很少出现 → TF-IDF 值高 → 这是该文档的关键词
- 余弦相似度：通过 TF-IDF 向量计算文档间的相似程度

**配图**：一个简单的例子，比如"Spark"这个词在不同文档中的 TF-IDF 值

---

## 第 13 页：TF-IDF 分布式实现

**要点（流程图）：**
```
文档目录 → wholeTextFiles (分布式读取)
     ↓
文本预处理（分布式）
  ├── 转小写 + 去标点
  ├── Tokenizer 分词
  └── StopWordsRemover 去停用词
     ↓
TF-IDF 计算（Spark MLlib Pipeline）
  ├── HashingTF: 分布式词频统计
  └── IDF: 分布式逆文档频率
     ↓
余弦相似度计算
     ↓
输出 Top-K 相似文档对 + 关键词
```

**关键点强调：**
- 使用 Spark MLlib 内置组件，工业级实现
- HashingTF 用哈希避免全局词典，适合大规模数据

---

## 第 14 页：TF-IDF 实验结果

**要点：**
- 测试数据：6 篇英文文档（机器学习、分布式系统、云计算、大数据、神经网络、Spark）
- 特征维度：1024
- 耗时：11.39 秒

**表格（Top 5 相似对）：**

| 排名 | 文档 A | 文档 B | 相似度 |
|------|--------|--------|--------|
| 1 | machine_learning | neural_networks | 0.1854 |
| 2 | big_data | spark_framework | 0.1665 |
| 3 | cloud_computing | big_data | 0.0892 |
| 4 | neural_networks | spark_framework | 0.0802 |
| 5 | machine_learning | big_data | 0.0787 |

**分析**：主题相近的文档相似度更高，TF-IDF 有效提取了文档主题特征

**配图**：放 TF-IDF 终端输出截图

---

## 第 15 页：任务运行监控

**要点：**
- Spark 提供 Web UI 实时监控
- 可查看：集群状态、任务进度、资源使用
- 任务分发到 2 个 Worker 并行执行

**配图**：放你截的第二张截图（Running Applications 有记录的那张），标注以下信息：
- 指出 "Running Applications (1)" → 正在运行的任务
- 指出 "Completed Applications (2)" → 已完成的任务
- 指出两个 Worker 的 Cores 和 Memory 全部被占满

---

## 第 16 页：分布式特性分析

**要点（对比表格）：**

| 特性 | 说明 |
|------|------|
| 数据并行 | 数据自动拆分到多个 Worker 并行处理 |
| 任务调度 | Master 自动分配任务，Worker 动态领取 |
| 容错机制 | RDD Lineage：数据丢失可通过血统信息重新计算 |
| 可扩展性 | 增加 Worker 节点即可线性扩展算力 |
| Shuffle | 跨节点数据交换，迭代算法的性能瓶颈 |

---

## 第 17 页：总结

**要点：**
1. 成功部署了 Spark 分布式集群（1 Master + 2 Workers）
2. 实现了两个经典分布式算法：PageRank（图迭代计算） + TF-IDF（文本分析）
3. 通过 Web UI 验证了任务确实分布到多节点并行执行
4. Docker 容器化方案与真实集群架构一致，便于快速复现

**收获：**
- 理解了 Master-Worker 架构的调度机制
- 掌握了 RDD 编程模型和 MLlib 机器学习组件
- 体会了分布式计算中 Shuffle 的开销和优化策略

---

## 第 18 页：展望（可选）

**要点：**
- 可扩展到更多 Worker 节点测试加速比
- 可使用更大规模数据集（如维基百科、真实网页图）
- 可对比 Spark 与 Hadoop MapReduce 的性能差异
- 可结合 Spark Streaming 实现实时流处理

---

## 第 19 页：致谢 / Q&A

- 感谢老师指导
- 参考文献（列 3-4 篇）：
  1. Zaharia M, et al. "Spark: Cluster Computing with Working Sets." HotCloud, 2010.
  2. Page L, et al. "The PageRank Citation Ranking." Stanford, 1999.
  3. Salton G, Buckley C. "Term-weighting approaches in automatic text retrieval." 1988.
  4. Apache Spark 官方文档：https://spark.apache.org/docs/latest/
- **Q&A**
