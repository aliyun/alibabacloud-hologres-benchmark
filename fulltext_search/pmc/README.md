# PMC 数据集

## 数据集简介

美国公共医学中心（英语：PubMed Central，缩写PMC），是一个免费的数字资源库，用于存档已发表于生物医学和生命科学期刊的开放获取全文学术文章。本数据集包含经过筛选的学术文章子集，适用于评估搜索引擎和数据库的全文检索性能。包含 57 万条记录，原始数据大小约为 22GB。

> 数据来源参考：[pmc](https://github.com/elastic/rally-tracks/blob/master/pmc)

### 数据集字段

| 字段名 | 类型 | 描述 |
|--------|------|------|
| `id` | BIGINT | 记录唯一标识符 |
| `name` | TEXT | 文章名称 |
| `journal` | TEXT | 期刊名称 |
| `date` | TEXT | 发表日期 |
| `volume` | TEXT | 卷号 |
| `issue` | TEXT | 期号 |
| `accession` | TEXT | PMC 访问号 |
| `timestamp` | TIMESTAMPTZ | 时间戳 |
| `pmid` | INTEGER | PubMed ID |
| `body` | TEXT | 文章正文内容 |

### 示例文档

```json
{
  "name": "3_Biotech_2015_Dec_13_5(6)_1007-1019",
  "journal": "3 Biotech",
  "date": "2015 Dec 13",
  "volume": "5(6)",
  "issue": "1007-1019",
  "accession": "PMC4624133",
  "timestamp": "2015-10-30 20:08:11",
  "pmid": "",
  "body": "\n==== Front\n3 Biotech3 Biotech3 Biotech2190-572X2190-5738Springer ..."
}
```

### 数据集下载

| 文件名 | 下载链接 |
|--------|----------|
| documents.json.bz2 | [下载](https://rally-tracks.elastic.co/pmc/documents.json.bz2) |

下载后使用 `bunzip2` 解压：

```bash
bunzip2 documents.json.bz2
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
