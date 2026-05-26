# 部署配置测试报告

## 1. 系统环境

| 项目 | 配置 |
|------|------|
| 操作系统 | Ubuntu 22.04 LTS |
| Docker 版本 | 20.10+ |
| Docker Compose 版本 | 2.0+ |
| 可用内存 | ≥ 4GB |
| 网络模式 | Bridge (spark-network) |

## 2. 集群配置

### 2.1 Spark Master 配置

| 参数 | 值 | 说明 |
|------|------|------|
| SPARK_MODE | master | 主节点角色 |
| SPARK_MASTER_HOST | spark-master | 主节点主机名 |
| SPARK_MASTER_PORT | 7077 | RPC 通信端口 |
| SPARK_MASTER_WEBUI_PORT | 8080 | Web UI 端口 |

### 2.2 Spark Worker 配置

| 参数 | 值 | 说明 |
|------|------|------|
| SPARK_MODE | worker | 工作节点角色 |
| SPARK_MASTER_URL | spark://spark-master:7077 | 连接主节点地址 |
| SPARK_WORKER_MEMORY | 1G | 每个 Worker 内存 |
| SPARK_WORKER_CORES | 2 | 每个 Worker 核心数 |
| Worker 数量 | 2 | 模拟分布式环境 |

### 2.3 网络配置

```yaml
networks:
  spark-network:
    driver: bridge
```

所有容器在同一 bridge 网络中，通过主机名互相访问。

### 2.4 数据卷映射

| 容器路径 | 宿主机路径 | 用途 |
|----------|-----------|------|
| /opt/spark-apps | ./src | 应用程序代码 |
| /opt/spark-data | ./data | 数据文件 |

## 3. 部署步骤

### 3.1 环境检查

```bash
# 检查 Docker 版本
docker --version
# Docker version 20.10.x

# 检查 Docker Compose 版本
docker-compose --version
# Docker Compose version v2.x.x

# 检查可用资源
docker system info | grep -E "CPUs|Total Memory"
```

### 3.2 拉取镜像

```bash
docker pull bitnami/spark:3.5.1
```

### 3.3 启动集群

```bash
cd docker/
docker-compose up -d
```

### 3.4 验证集群状态

```bash
# 查看容器状态
docker-compose ps

# 预期输出:
# NAME             IMAGE                COMMAND    STATUS    PORTS
# spark-master     bitnami/spark:3.5.1  ...       Up        0.0.0.0:7077->7077/tcp, 0.0.0.0:8080->8080/tcp
# spark-worker-1   bitnami/spark:3.5.1  ...       Up        0.0.0.0:8081->8081/tcp
# spark-worker-2   bitnami/spark:3.5.1  ...       Up        0.0.0.0:8082->8081/tcp
```

### 3.5 Web UI 验证

- 访问 http://localhost:8080 确认 Master 正常运行
- 确认 2 个 Worker 已注册并处于 ALIVE 状态
- 确认集群总资源: 4 Cores, 2.0 GB Memory

## 4. 功能模块测试

### 4.1 Spark Master 测试

| 测试项 | 结果 | 说明 |
|--------|------|------|
| Web UI 可访问 | ✓ | http://localhost:8080 正常加载 |
| Worker 注册 | ✓ | 2 个 Worker 成功注册 |
| 资源分配 | ✓ | 4 Cores / 2GB 正确显示 |
| 任务调度 | ✓ | spark-submit 任务正确分配到 Worker |

### 4.2 Spark Worker 测试

| 测试项 | 结果 | 说明 |
|--------|------|------|
| Worker 1 状态 | ✓ | ALIVE, 2 cores, 1GB |
| Worker 2 状态 | ✓ | ALIVE, 2 cores, 1GB |
| 数据卷访问 | ✓ | /opt/spark-data 可读写 |
| 程序文件访问 | ✓ | /opt/spark-apps 可读 |

### 4.3 网络连通性测试

```bash
# 从 Worker 1 ping Master
docker exec spark-worker-1 ping -c 3 spark-master
# 结果: 3 packets transmitted, 3 received, 0% packet loss

# 从 Worker 2 ping Master
docker exec spark-worker-2 ping -c 3 spark-master
# 结果: 3 packets transmitted, 3 received, 0% packet loss
```

### 4.4 任务提交测试

```bash
# 提交 PageRank 任务
docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark-apps/pagerank.py \
    /opt/spark-data/web_graph.txt 10

# 结果: 任务成功完成，输出 PageRank 排名
```

## 5. 性能基准

| 指标 | PageRank (20节点/10迭代) | TF-IDF (6文档) |
|------|--------------------------|----------------|
| 执行时间 | ~15s | ~20s |
| 内存使用峰值 | ~300MB | ~400MB |
| 任务数 | 10 stages | 8 stages |
| Shuffle 数据量 | ~5KB | ~10KB |

> 注：Docker 环境下性能受限于虚拟化开销，实际分布式环境性能更优。

## 6. 常见问题与解决

| 问题 | 原因 | 解决方案 |
|------|------|----------|
| Worker 无法连接 Master | 网络配置错误 | 确保所有容器在同一 network |
| 内存不足 | 宿主机资源不足 | 减少 SPARK_WORKER_MEMORY |
| 端口冲突 | 端口被占用 | 修改 docker-compose.yml 中的端口映射 |
| 镜像拉取失败 | 网络问题 | 使用国内镜像源或代理 |

## 7. 结论

Spark 分布式集群通过 Docker Compose 成功部署，所有功能模块测试通过：
- Master-Worker 架构正常运行
- 任务调度和资源分配正确
- 网络连通性良好
- 两个分布式计算程序（PageRank、TF-IDF）均可正常提交和执行
