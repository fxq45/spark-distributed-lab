# 视频演示流程脚本

> **主题**：基于 Apache Spark 的分布式计算实验 — 现场部署与程序演示  
> **总时长**：8-9 分钟（留 1-2 分钟余量）  
> **前置条件**：已安装 Docker（内置 Compose 插件），已克隆项目代码

---

## 第一部分：开场介绍（约 30 秒）

**操作**：打开终端，`cd` 到项目目录 （在C:\GitRepos\spark-distributed-lab文件夹下选择在终端中打开，然后输入wsl）
启动docker desktop

**口述**：
> 大家好，我的演示主题是"基于 Apache Spark 的分布式计算实验"。
> 本实验使用 Docker 部署 Spark 分布式集群，包含 1 个 Master 和 2 个 Worker 节点，
> 并实现了两个经典分布式算法：PageRank 和 TF-IDF 文本相似度分析。
> 下面我进行现场部署和运行演示。

---

## 第二部分：展示项目结构（约 1 分钟）

**操作**：在终端依次执行

```bash

# （如果没有 tree 命令，用 ls 代替）
ls -la
ls docker/
ls src/
ls data/
ls scripts/
```

**口述**：
> 先看一下项目结构。
> - `docker/` 目录存放 Dockerfile 和 docker-compose.yml，定义集群配置
> - `src/` 下有两个核心程序：pagerank.py 和 tfidf_similarity.py
> - `data/` 存放测试数据：web_graph.txt 是 PageRank 的网页链接图，documents/ 目录下是 6 篇 TF-IDF 测试文档
> - `scripts/` 是一键启动、停止和运行的脚本

**（可选加分项）** 快速打开 `docker-compose.yml` 说明集群配置：

```bash
cat docker/docker-compose.yml
```

> docker-compose 定义了 3 个服务：spark-master、spark-worker-1、spark-worker-2。
> 每个 Worker 分配 1G 内存、2 核 CPU，通过 Docker 虚拟网络互相通信。

---

## 第三部分：启动 Spark 集群（约 1.5-2 分钟）

**操作**：

```bash
./scripts/start-cluster.sh
```

**口述**（等待构建和启动的过程中）：
> 运行启动脚本。首先它会构建 Docker 镜像——基础镜像是 Ubuntu 22.04，安装了 OpenJDK 17 和 Spark 3.5.8。Spark 从清华镜像站下载，不需要访问外网。
>
> （镜像构建完成后）镜像构建好了，正在启动 3 个容器...
>
> （容器启动后）可以看到 3 个容器都是 running 状态。

**操作**：打开浏览器，访问 Spark Master Web UI

```
http://localhost:8080
```

**口述**：
> 打开 Spark Master 的 Web UI 来验证集群状态。
> 可以看到：
> - 2 个 Worker 状态都是 **ALIVE**
> - 总共 **4 核 CPU**、**2 GB 内存**
> - 当前没有运行中的任务
>
> 集群部署成功，接下来运行算法。

---

## 第四部分：运行 PageRank（约 2 分钟）

**操作**：回到终端执行

```bash
./scripts/run-pagerank.sh
```

**口述**（提交任务后）：
> 通过 spark-submit 将 PageRank 程序提交到集群。
> 参数说明：master 指向 spark-master 的 7077 端口，输入数据是 web_graph.txt，迭代 10 次。

**等待运行完成，讲解输出结果**：
> 运行完成了，看一下结果：
> - 图信息：**20 个节点，84 条边**
> - 阻尼系数 d = 0.85
> - 经过 10 次迭代计算，得到 Top 20 的 PageRank 排名
> - 排名第一的是节点 **G**，PageRank 值为 0.06781
> - 第二名是节点 **I**，第三名是节点 **D**
> - PageRank 总和为 **1.000000**，验证了计算的正确性
>
> 分析：中心节点 G、I、D 在图中被较多节点链接，所以 PageRank 值较高，这符合算法预期。

### 4.1 查看 Web UI — Executor 分布（约 1 分钟）

**操作**：切换到浏览器，打开 `http://localhost:8080`，点击 Completed Applications 里的 Application 链接（如 `app-20260602032047-0000`）

**口述**：
> 回到浏览器，在 Spark Master 页面可以看到刚才完成的 Application。点进去看详情。
>
> （指向 Executors 表格）这里显示 Spark 一共创建了 **4 个 Executor**，分布在 2 个 Worker 节点上。
> 这两个 IP 地址分别对应 Worker-1 和 Worker-2，每个 Worker 上运行了 2 个 Executor。
>
> 这是因为我们给每个 Worker 配了 2 核 CPU、每个 Executor 用 1 核，所以每个 Worker 上分配了 2 个 Executor，总共 2 × 2 = 4 个。
>
> （指向 State 列）状态显示 KILLED，这不是报错，而是任务已经执行完毕，Executor 被正常回收释放资源了。
>
> 这个页面直观地证明了任务确实是**分布到多个节点并行执行**的，而不是在单机上跑的。

**（可选）** 如果时间充裕，点击 Stages 标签页：
> 在 Stages 标签页可以看到任务被拆分成了多个 Stage，每个 Stage 包含多个 Task，这些 Task 分别在不同的 Executor 上并行执行。这就是 Spark 的 Job → Stage → Task 三级调度机制。

---

## 第五部分：运行 TF-IDF 文本相似度分析（约 2 分钟）

**操作**：

```bash
./scripts/run-tfidf.sh
```

**口述**：
> 接下来运行 TF-IDF 文本相似度分析。
> 输入数据是 6 篇英文文档，主题分别涉及机器学习、分布式系统、云计算、大数据、神经网络和 Spark 框架。
> 特征维度设置为 1024。

**等待运行完成，讲解输出结果**：
> 运行完成，耗时约 11 秒。看一下 Top 5 相似文档对：
> - 第一名：**machine_learning** 和 **neural_networks**，相似度 0.1854 — 因为它们都涉及深度学习、模型训练等关键词
> - 第二名：**big_data** 和 **spark_framework**，相似度 0.1665 — Spark 本身就是大数据处理框架，主题高度相关
> - 第三名：**cloud_computing** 和 **big_data**，相似度 0.0892
>
> 同时，程序还输出了各文档的 Top 5 关键词（按 TF-IDF 权重排序），说明 TF-IDF 有效提取了每篇文档的主题特征。
>
> 统计信息：6 个文档共 15 个文档对，平均相似度 0.057，最高 0.1854，最低 0.0014。

---

## 第六部分：分布式特性简要分析（约 30 秒）

**口述**（可以配合 PPT 或直接讲）：
> 简单总结下本实验体现的分布式特性：
> 1. **数据并行**：20 个节点的边数据自动分片到 2 个 Worker 并行处理
> 2. **任务调度**：PageRank 每轮迭代生成 Stage，Master 将 Task 分配给各 Executor
> 3. **内存缓存**：通过 `cache()` 将静态邻接表常驻内存，避免 10 轮迭代重复读取
> 4. **MLlib Pipeline**：TF-IDF 使用 Spark 内置的 HashingTF + IDF，自动分布式计算

---

## 第七部分：停止集群 + 结尾（约 30 秒）

**操作**：

```bash
./scripts/stop-cluster.sh
```

**口述**：
> 演示到此结束。最后停止集群，释放资源。
>
> 总结：本实验通过 Docker 部署了 Spark 分布式集群，成功实现并验证了 PageRank 和 TF-IDF 两个经典分布式算法。
> 谢谢大家，请老师指导。

---

## 时间分配汇总

| 环节 | 内容 | 预计时长 |
|------|------|----------|
| 1 | 开场介绍 | 30 秒 |
| 2 | 项目结构展示 | 1 分钟 |
| 3 | 启动集群 + Web UI 验证 | 1.5-2 分钟 |
| 4 | 运行 PageRank + 结果讲解 + Web UI Executor 分布 | 3 分钟 |
| 5 | 运行 TF-IDF + 结果讲解 | 2 分钟 |
| 6 | 分布式特性分析 | 30 秒 |
| 7 | 停止集群 + 结尾 | 30 秒 |
| **总计** | | **约 8-9 分钟** |

---

## 注意事项 / Tips

1. **提前准备**：建议在录制前先运行一遍 `start-cluster.sh`，让 Docker 镜像已经构建好（避免首次构建等待 3-5 分钟）。录制时第二次启动会很快（几秒钟）。
2. **终端字体**：把终端字体调大（至少 16pt），确保视频中能看清命令和输出。
3. **浏览器标签**：提前在浏览器打开 `http://localhost:8080` 标签页，集群启动后直接刷新即可。
4. **如果程序等待时间太长**：在等待过程中可以口头讲解算法原理或代码结构，不要沉默。
5. **万一出错**：如果某个步骤出错，保持冷静，说明可能的原因并重试。实际演示中 Docker 偶尔有网络延迟是正常的。
6. **展示 Web UI 时**：可以鼠标指向关键信息（Workers、Cores、Memory、Application 列表），帮助观众定位。
