# Hologres Benchmark

[Hologres](https://www.alibabacloud.com/product/hologres) 官方文档公开的性能测试脚本集合，用于评估 Hologres 在不同场景下的性能表现。

## 基准测试列表

| 测试场景 | 数据集 | 数据量 | 说明 |
|---------|--------|--------|------|
| [全文检索 - http_logs](fulltext_search/http_logs/) | 1998 世界杯 HTTP 访问日志 | 2.47 亿条 / ~32GB | 基于 Elastic rally-tracks 的 http_logs 数据集，评估全文检索与日志分析性能 |

## 项目结构

```
hologres_benchmark/
├── README.md
└── fulltext_search/                  # 全文检索场景
    └── http_logs/                    # http_logs 数据集
        ├── README.md                 # 详细使用说明
        ├── config.json               # 数据库连接配置模板
        ├── benchmark_queries.yaml    # 基准测试查询定义（20 条查询）
        ├── hologres_benchmark.py     # 基准测试主程序
        ├── hologres_data_import.py   # 多进程数据导入工具
        ├── ndjson_to_csv.py          # NDJSON 转 CSV 辅助工具
        └── requirements.txt          # Python 依赖
```

## 快速开始

各测试场景的详细使用说明请参考对应目录下的 README：

- [全文检索 - http_logs](fulltext_search/http_logs/README.md)

## License

Apache License 2.0
