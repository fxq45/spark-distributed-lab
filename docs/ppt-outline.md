# PPT 大纲 —— 基于 Apache Spark 的分布式计算实验

> 完整指令：请根据以下大纲生成一份 PPT，共 15-18 页。风格简洁专业，配色以深蓝+白色为主，科技感。每页内容不要太多文字，多用图表和关键词。10分钟演讲节奏。

---

## PPT 验收标准（发给 Kimi 的要求）

1. **总页数**：15-18 页（不含封面和结束页）
2. **每页文字**：标题 + 不超过 5 个要点，每个要点不超过 20 字
3. **必须包含的图**：系统架构图（Master-Worker）、Spark 运行架构图（Driver-Executor）、Job/Stage/Task 调度图、PageRank 算法流程图、TF-IDF 计算流程图、实验结果表格
4. **配色**：科技蓝（#1E3A5F）+ 白色背景，图表用蓝橙对比色
5. **字体**：标题用粗体 28-32pt，正文 18-20pt，代码用等宽字体 16pt
6. **动画**：不要花哨动画，简单的逐条出现即可
7. **演讲时间**：控制在 10 分钟内（每页约 30-40 秒）
8. **截图占位**：凡是标注 `【插入截图 X】` 的地方，请生成空白占位框（带虚线边框 + 文字提示），我会后续手动替换为真实截图

---

## 需要插入的截图清单（共 1 张）

| 编号 | 截图内容 | 插入位置 | 占页面比例 |
|------|----------|----------|-----------|
| 截图 1 | Spark 集群空闲状态 Web UI（2 Workers ALIVE、4 Cores） | 第 10 页 | 60% |

> 注：算法运行结果截图（终端输出、任务运行中的 Web UI 等）留给视频演示环节，PPT 中用表格/文字呈现关键数据即可。

---

## 第 1 页：封面

- **标题**：基于 Apache Spark 的分布式计算实验
- **副标题**：典型分布式（云）技术及系统 · 课程实践
- **信息**：姓名、学号、日期
- **背景**：放一个 Spark Logo + 简单的集群网络示意图

---

## 第 2 页：目录

- 一、选题背景与目标
- 二、Apache Spark 简介与核心概念
- 三、Spark 运行架构与调度机制
- 四、容错机制与 Hadoop 对比
- 五、实验集群设计与部署
- 六、分布式算法实现（PageRank + TF-IDF）
- 七、实验结果与分布式特性分析
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

## 第 6 页：Spark 运行架构（重点页）

**要点：**
- **Driver**：运行用户主程序，创建 SparkContext，负责任务拆分与调度
- **Cluster Manager**：集群资源管理器（Standalone / YARN / Mesos / K8s）
- **Executor**：运行在 Worker 节点上的进程，执行具体 Task，管理本地缓存
- 数据流：Driver → Cluster Manager（申请资源）→ Executor（执行任务）→ Driver（返回结果）

**配图（核心页，重点画好）：**
```
┌─────────────────────────────────────────────┐
│               Spark Application             │
│                                             │
│  ┌───────────────────┐                      │
│  │     Driver         │                      │
│  │  SparkContext      │                      │
│  │  DAGScheduler      │                      │
│  │  TaskScheduler     │                      │
│  └────────┬──────────┘                      │
│           │  申请资源                        │
│  ┌────────▼──────────┐                      │
│  │  Cluster Manager   │                      │
│  │ (Standalone Master)│                      │
│  └────┬──────────┬───┘                      │
│       │          │  分配 Executor            │
│  ┌────▼───┐ ┌───▼────┐                     │
│  │Executor│ │Executor│                      │
│  │(Worker1)│ │(Worker2)│                     │
│  │Task|Task│ │Task|Task│                     │
│  │ Cache   │ │ Cache   │                     │
│  └────────┘ └────────┘                      │
└─────────────────────────────────────────────┘
```

**讲解重点**：区分 Driver / Cluster Manager / Executor 三层角色

---

## 第 7 页：任务调度机制 — Job / Stage / Task

**要点：**
- **Application**：一个 SparkContext 对应一个 Application
- **Job**：每次 Action 触发一个 Job
- **Stage**：以 Shuffle 为边界，将 Job 切分为多个 Stage（宽依赖 vs 窄依赖）
- **Task**：每个 Stage 中每个 Partition 对应一个 Task，分发到 Executor 并行执行

**配图（调度拆分示意图）：**
```
Action (collect)
   │
   ▼
 Job 0
   │
   ├── Stage 0 (窄依赖: textFile → map → filter)
   │      ├── Task 0.0 (Partition 0)  → Executor 1
   │      └── Task 0.1 (Partition 1)  → Executor 2
   │                ↓ Shuffle
   └── Stage 1 (reduceByKey → collect)
          ├── Task 1.0 (Partition 0)  → Executor 1
          └── Task 1.1 (Partition 1)  → Executor 2
```

**讲解重点**：Shuffle 是 Stage 切分的依据，也是性能瓶颈所在

---

## 第 8 页：容错机制 — RDD Lineage 与 Hadoop 对比

**要点：**
- **RDD Lineage（血统）**：每个 RDD 记录其由哪些父 RDD 经过何种 Transformation 得到
- 数据丢失时，根据 Lineage 从上游重新计算，无需写多副本
- **checkpoint**：对于 Lineage 链过长的场景，手动持久化到 HDFS 截断链路

**与 Hadoop MapReduce 对比表格：**

| 特性 | Hadoop MapReduce | Spark |
|------|------------------|-------|
| 计算模型 | 两阶段（Map → Reduce） | DAG（多阶段流水线） |
| 中间结果 | 写磁盘（HDFS） | 内存缓存（RDD cache） |
| 迭代效率 | 每轮读写磁盘，慢 | 内存复用，快 10-100× |
| 容错 | HDFS 3 副本冗余 | RDD Lineage 重算 |
| 适用场景 | 单次批处理 | 迭代计算 / 交互查询 |

**讲解重点**：Spark 的 Lineage 容错 = 用计算换存储，适合迭代场景

---

## 第 9 页：实验集群架构设计

**要点：**
- Master-Worker 架构（Standalone 模式）
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

## 第 10 页：环境部署与验证

**要点：**
- 使用 Docker + Docker Compose 一键部署
- 基础镜像：Ubuntu 22.04 + OpenJDK 17 + Spark 3.5.8
- 自构建镜像，不依赖外部仓库
- 一条命令启动集群：`./scripts/start-cluster.sh`
- 部署验证：Spark Master Web UI 显示 2 Workers ALIVE、4 Cores、2 GB Memory

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

**【插入截图 1：Spark 集群空闲状态 Web UI】**
> 截图内容：Spark Master 页面，显示 2 Workers ALIVE、4 Cores、2GB Memory、Running Applications = 0
> 用途：证明集群部署成功
> 建议占据页面 40% 面积，底部放置

---

## 第 11 页：算法一 — PageRank 原理与分布式实现

**原理要点：**
- Google 1998 年提出的网页排名算法
- 核心思想：被更多重要页面链接的页面更重要
- 迭代公式：PR(A) = (1-d)/N + d × Σ(PR(Ti)/C(Ti))
  - d = 0.85（阻尼系数）、N = 节点总数

**分布式实现（用精简流程图）：**
```
textFile → groupByKey (邻接表)
  → 迭代 10 次:  join → flatMap (贡献值) → reduceByKey (Shuffle 汇总) → mapValues (阻尼更新)
  → 输出 Top-K
```

**关键点强调：**
- `flatMap` + `reduceByKey` = 经典 MapReduce 模式
- `cache()` 缓存静态链接数据，避免重复读取
- 每轮迭代触发 1 次 Shuffle，是性能瓶颈

**配图**：一个 4-5 个节点的小图，标注箭头方向和 PR 值

---

## 第 12 页：PageRank 实验结果

**要点：**
- 测试数据：20 个节点，84 条边
- 迭代次数：10
- 耗时：8.95 秒
- PR 总和 = 1.000000（验证正确性）

**表格（Top 5）：**

| 排名 | 节点 | PageRank |
|------|------|----------|
| 1 | G | 0.06781 |
| 2 | I | 0.06373 |
| 3 | D | 0.06157 |
| 4 | E | 0.05980 |
| 5 | R | 0.05642 |

> 完整 Top 20 排名及终端运行截图将在视频演示环节展示。

---

## 第 13 页：算法二 — TF-IDF 原理与分布式实现

**原理要点：**
- TF-IDF = 词频 × 逆文档频率（信息检索经典算法）
- TF(t,d) = 词 t 在文档 d 中的出现次数 / 文档 d 总词数
- IDF(t) = log(文档总数 / 包含词 t 的文档数)
- 余弦相似度：通过 TF-IDF 向量计算文档间的相似程度

**分布式实现（用精简流程图）：**
```
wholeTextFiles → 预处理 (分词/去停用词)
  → HashingTF (分布式词频) → IDF (分布式逆文档频率)
  → 余弦相似度 → 输出 Top-K 相似文档对
```

**关键点强调：**
- 使用 Spark MLlib 内置 Pipeline，工业级实现
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

> 完整关键词列表及终端运行截图将在视频演示环节展示。

---

## 第 15 页：分布式特性分析（结合实验数据）

**要点（对比表格，结合本实验的实际数据）：**

| 特性 | 本实验中的体现 |
|------|----------------|
| 数据并行 | PageRank 20 节点的边数据自动拆分到 2 个 Worker 并行处理 |
| 任务调度 | 每轮 PageRank 迭代生成 1 个 Stage，Master 分配 Task 到各 Executor |
| Shuffle 开销 | PageRank `reduceByKey` 每轮触发跨节点数据交换；10 轮迭代 = 10 次 Shuffle |
| 内存缓存 | `cache()` 将静态邻接表常驻内存，避免 10 轮重复读取（对比 Hadoop 每轮都要读写 HDFS） |
| MLlib Pipeline | TF-IDF 使用 HashingTF + IDF 组合，自动分发到各 Worker 并行计算特征向量 |

**讲解重点**：不是泛泛地讲分布式特性，而是指出"我们的实验中哪里体现了这些特性"

---

## 第 16 页：总结

**要点：**
1. 成功部署了 Spark 分布式集群（1 Master + 2 Workers，Standalone 模式）
2. 实现了两个经典分布式算法：PageRank（图迭代计算） + TF-IDF（文本分析）
3. 通过 Web UI 验证了任务确实分布到多节点并行执行
4. Docker 容器化方案与真实集群架构一致，便于快速复现

**收获：**
- 理解了 Driver / Executor / Cluster Manager 三层运行架构
- 掌握了 Job → Stage → Task 的调度拆分过程
- 体会了 RDD Lineage 容错与 Hadoop 副本容错的设计差异
- 掌握了 RDD 编程模型和 MLlib 机器学习组件

---

## 第 17 页：展望（可选）

**要点：**
- 可扩展到更多 Worker 节点测试加速比
- 可使用更大规模数据集（如维基百科、真实网页图）
- 可对比 Spark 与 Hadoop MapReduce 的性能差异
- 可结合 Spark Streaming 实现实时流处理

---

## 第 18 页：致谢 / Q&A

- 感谢老师指导
- 参考文献（列 3-4 篇）：
  1. Zaharia M, et al. "Spark: Cluster Computing with Working Sets." HotCloud, 2010.
  2. Page L, et al. "The PageRank Citation Ranking." Stanford, 1999.
  3. Salton G, Buckley C. "Term-weighting approaches in automatic text retrieval." 1988.
  4. Apache Spark 官方文档：https://spark.apache.org/docs/latest/
- **Q&A**
