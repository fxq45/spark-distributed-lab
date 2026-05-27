FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV SPARK_VERSION=3.5.8
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV PYSPARK_PYTHON=python3

# 安装依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    python3 \
    python3-pip \
    python3-numpy \
    curl \
    procps \
    tini \
    && rm -rf /var/lib/apt/lists/*

# 下载 Spark（清华镜像优先，Apache归档备用）
RUN ( curl -fSL "https://mirrors.tuna.tsinghua.edu.cn/apache/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -o /tmp/spark.tgz \
    || curl -fSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -o /tmp/spark.tgz ) \
    && tar -xzf /tmp/spark.tgz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && rm /tmp/spark.tgz

# 创建工作目录
RUN mkdir -p /opt/spark-apps /opt/spark-data

# 配置 Spark 日志级别
RUN cp ${SPARK_HOME}/conf/log4j2.properties.template ${SPARK_HOME}/conf/log4j2.properties \
    && sed -i 's/rootLogger.level = info/rootLogger.level = warn/' ${SPARK_HOME}/conf/log4j2.properties

WORKDIR ${SPARK_HOME}

ENTRYPOINT ["tini", "--"]
