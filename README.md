# In-Database Time Series Clustering on Apache IoTDB

本项目是对 SIGMOD 2025 论文 **《In-Database Time Series Clustering》** 的算法复现与优化扩展。我们在 Apache IoTDB 0.13.3 上成功复现了论文提出的 K-Shape 和 Medoid-Shape 两种数据库内时序聚类算法，并**创新性地扩展了 Chunk（块）级别的聚类功能及混合元数据加速**，在保证聚类精度的前提下显著提升了大规模时序数据的聚类查询效率。

## 快速开始

### 环境要求

- Java 8+
- Maven 3.6+
- Windows / Linux / macOS

### 编译

```bash
cd iotdb-0.13.3
mvn clean package -pl distribution -am -DskipTests
```

编译成功后，可运行二进制包位于 `distribution/target/apache-iotdb-0.13.3-all-bin.zip`。

### 配置

解压二进制包，编辑 `conf/iotdb-engine.properties`，在文件末尾添加（若使用 air 数据集）：

```properties
cluster_num=3
seq_length=166
```

### 启动服务

```bash
# Windows
sbin\start-server.bat

# Linux / macOS
bash sbin/start-server.sh
```

### 启动客户端

```bash
# Windows
sbin\start-cli.bat -h 127.0.0.1

# Linux / macOS
bash sbin/start-cli.sh -h 127.0.0.1
```

### 导入数据

下载数据集：UCR-Air 数据集（ChlorineConcentration）可从 [UCR Time Series Classification Archive](https://www.cs.ucr.edu/~eamonn/time_series_data_2018/) 下载。

导入数据：

```bash
cd tools
.\import-csv.bat -h 127.0.0.1 -p 6667 -u root -pw root -f "path/to/air.csv"
```

### 执行聚类查询

```sql
-- K-Shape Page 级别
SELECT lsmKShape(s0) FROM root.air.d0;

-- K-Shape Chunk 级别
SELECT lsmKShape(s0,"level"="chunk") FROM root.air.d0;

-- M-Shape Page 级别
SELECT lsmMShape(s0) FROM root.air.d0;

-- M-Shape Chunk 级别
SELECT lsmMShape(s0,"level"="chunk") FROM root.air.d0;

-- 范围查询
SELECT lsmKShape(s0,"level"="chunk") FROM root.ecg.d0 WHERE time >= 0 AND time <= 233333;
```

## 主要贡献

### 算法复现

在 Apache IoTDB 0.13.3 上成功复现论文的 In-Database K-Shape 和 Medoid-Shape 聚类算法，修复原代码在新版 IoTDB 上的编译与运行问题。

### Chunk 级别聚类扩展（创新点）

- 设计并实现 Chunk 级别的元数据生成与查询
- 利用 Chunk 内 Page 预计算元数据生成 Chunk 级精确元数据
- 设计实现多层元数据混合加速算法
- 查询时合并次数从 93 次降至 8 次（Air 数据集）

### 工程问题解决

- 修复元数据序列化标志位丢失问题
- 修复浅拷贝导致的缓存数据污染
- 处理单 Chunk 文件兼容性问题

## 数据集

实验使用 UCR Time Series Classification Archive 中的 Air 数据集（ChlorineConcentration）。

| 属性 | 数值 |
| --- | --- |
| 子序列数量 | 4,307 |
| 子序列长度 | 166 |
| 类别数 | 3 |
| 数据来源 | 空气质量传感器 |

## 引用

如果本工作对你的研究有帮助，请引用原论文：

Yunxiang Su, Kenny Ye Liang, and Shaoxu Song. 2025. In-Database Time Series Clustering. *Proc. ACM Manag. Data* 3, 1 (SIGMOD), Article 46 (February 2025), 26 pages. https://doi.org/10.1145/3709696

## 许可证

本项目基于 Apache License 2.0 开源。原 IoTDB 代码版权归 Apache 软件基金会所有。
