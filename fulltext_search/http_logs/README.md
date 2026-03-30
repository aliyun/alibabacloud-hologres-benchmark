# http_logs 数据集

## 数据集简介

数据集 `http_logs` 源自 1998 年世界杯官方网站的服务器访问日志。它包含 2.47 亿条记录，原始数据大小约为 32GB。每条记录包含 `@timestamp`（时间戳）、`clientip`（客户端 IP）、`request`（HTTP 请求）、`status`（状态码）和 `size`（响应大小）等字段。该数据集被广泛用作评估搜索引擎和数据库全文检索与分析性能的基准。

> 数据来源参考：[elastic/rally-tracks/http_logs](https://github.com/elastic/rally-tracks/tree/master/http_logs)

### 数据集字段

| 字段名 | 类型 | 描述 |
|--------|------|------|
| `id` | BIGINT | 记录唯一标识符 |
| `@timestamp` | BIGINT | 时间戳（Unix 时间戳格式） |
| `clientip` | TEXT | 客户端 IP 地址 |
| `request` | TEXT | HTTP 请求内容 |
| `status` | INTEGER | HTTP 状态码 |
| `size` | INTEGER | 响应大小（字节） |

### 数据集下载

| 文件名 | 下载链接 |
|--------|----------|
| documents-181998.json.bz2 | [下载](https://rally-tracks.elastic.co/http_logs/documents-181998.json.bz2) |
| documents-191998.json.bz2 | [下载](https://rally-tracks.elastic.co/http_logs/documents-191998.json.bz2) |
| documents-201998.json.bz2 | [下载](https://rally-tracks.elastic.co/http_logs/documents-201998.json.bz2) |
| documents-211998.json.bz2 | [下载](https://rally-tracks.elastic.co/http_logs/documents-211998.json.bz2) |
| documents-221998.json.bz2 | [下载](https://rally-tracks.elastic.co/http_logs/documents-221998.json.bz2) |
| documents-231998.json.bz2 | [下载](https://rally-tracks.elastic.co/http_logs/documents-231998.json.bz2) |
| documents-241998.json.bz2 | [下载](https://rally-tracks.elastic.co/http_logs/documents-241998.json.bz2) |

下载后使用 `bunzip2` 解压：

```bash
bunzip2 documents-*.json.bz2
```

## 用法

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置数据库

编辑 `config.json`，填写 Hologres 连接信息。

### 3. 运行基准测试

```bash
python3 hologres_benchmark.py \
  --config config.json \
  --data-dir /path/to/data \
  --queries-config benchmark_queries.yaml
```
