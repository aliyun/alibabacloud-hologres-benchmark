#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hologres简单基准测试工具
连接Hologres数据库，执行批量导入、VACUUM和查询测试
"""

import time
import json
import logging
import argparse
import os
import traceback
import yaml
from pathlib import Path
from datetime import datetime
from string import Template
from typing import Dict, List, Tuple, Any
from contextlib import contextmanager

try:
    import psycopg
    PSYCOPG_AVAILABLE = True
except ImportError:
    PSYCOPG_AVAILABLE = False
    print("警告: psycopg未安装，请运行: pip3 install psycopg[binary]")


class HologresBenchmark:
    """Hologres基准测试类"""
    
    def __init__(self, config: Dict[str, Any], queries_config_path: str = None):
        """初始化配置
        
        Args:
            config: 数据库连接配置
            queries_config_path: 查询配置文件路径（YAML格式）
        """
        self.config = config
        self.logger = self._setup_logger()
        self.queries_config = self._load_queries_config(queries_config_path)
    
    def _load_queries_config(self, config_path: str = None) -> Dict[str, Any]:
        """加载查询配置文件"""
        if config_path is None:
            # 默认查询配置文件路径
            current_dir = Path(__file__).parent
            config_path = current_dir.parent / 'benchmark_queries.yaml'
        else:
            config_path = Path(config_path)
        
        if not config_path.exists():
            raise FileNotFoundError(f"查询配置文件不存在: {config_path}")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        self.logger.info(f"已加载查询配置文件: {config_path}")
        return config
        
    def _setup_logger(self) -> logging.Logger:
        """设置日志记录器"""
        logger = logging.getLogger('HologresBenchmark')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    @contextmanager
    def get_connection(self):
        """获取数据库连接"""
        if not PSYCOPG_AVAILABLE:
            raise ImportError("psycopg未安装，请运行: pip3 install psycopg[binary]")
        
        conn = None
        try:
            conn = psycopg.connect(
                host=self.config['host'],
                port=self.config['port'],
                dbname=self.config['database'],
                user=self.config['username'],
                password=self.config['password'],
                connect_timeout=30,
                options="-c hg_experimental_enable_result_cache=off",
                application_name='hologres_benchmark'
            )
            conn.autocommit = True
            self.logger.info(f"成功连接到Hologres: {self.config['host']}:{self.config['port']}")
            yield conn
        except Exception as e:
            self.logger.error(f"连接失败: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
                self.logger.info("已关闭数据库连接")

    def create_table(self, cursor, table_name: str):
        """创建测试表"""
        tg_sql = f"CALL HG_CREATE_TABLE_GROUP('tg_6', 6, 'ignore');"
        create_sql = f"""
            DROP TABLE IF EXISTS {table_name};
            PURGE TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name} (
                id BIGINT NOT NULL,
                "@timestamp" BIGINT NOT NULL,
                clientip TEXT,
                request TEXT,
                status INTEGER,
                size INTEGER
            ) WITH (
                table_group = 'tg_6',
                bitmap_columns = 'status',
                segment_key = '"@timestamp"',
                clustering_key = '"@timestamp"'
            );
        """
        
        cursor.execute(tg_sql)
        cursor.execute(create_sql)
        self.logger.info(f"已创建表: {table_name}")

    def bulk_load_data(self, cursor, table_name: str, data_dir: str,
                        batch_size: int = 10000, num_parsers: int = 8) -> Tuple[float, int]:
        """批量导入测试数据
        
        Args:
            cursor: 数据库游标
            table_name: 目标表名
            data_dir: NDJSON 数据文件目录
            batch_size: 每批次的行数
            num_parsers: Parser 进程数
        """
        self.logger.info(f"开始导入测试数据 (data_dir={data_dir})...")
        start_time = time.time()

        # 使用 HologresDataImporter 导入数据
        from hologres_data_import import HologresDataImporter
        importer = HologresDataImporter(self.config, table_name)
        result = importer.import_data(data_dir, batch_size, num_parsers)
        
        if not result['success']:
            raise RuntimeError(f"数据导入失败: {result.get('error', '未知错误')}")
        
        insert_time = result['load_time']
        total_inserted = result['records_loaded']

        self.logger.info(f"数据导入完成, 开始创建索引")

        create_index_sql = f"""
            SELECT hg_admin_command('set_global_flag', 'fulltext_build_memory_budget_mb=128');
            CREATE INDEX IF NOT EXISTS {table_name}_request_idx ON {table_name}
            USING FULLTEXT (request) WITH (
                tokenizer = 'keyword',
                analyzer_params = '{{"tokenizer":{{"type":"keyword"}}}}'
            );
        """

        cursor.execute(create_index_sql)

        start_vacuum_time = time.time()
        cursor.execute(f"VACUUM {table_name};")
        cursor.execute(f"ANALYZE {table_name};")
        end_vacuum_time = time.time()

        end_time = time.time()
        load_time = end_time - start_time
        vacuum_time = end_vacuum_time - start_vacuum_time
        
        self.logger.info(f"数据导入完成: {total_inserted} 条记录, 总耗时: {load_time:.3f} 秒, 其中插入耗时: {insert_time:.3f} 秒, vacuum time {vacuum_time:.3f} 秒")

        # 查询表+索引总大小
        data_size = "unknown"
        data_size_sql = f"""
            SELECT * FROM pg_size_pretty(hologres.hg_relation_size('{table_name}', 'data'));
        """
        cursor.execute(data_size_sql)
        results = cursor.fetchall()
        if results is not None and len(results) > 0:
            data_size = str(results[0][0])
            self.logger.info(f"数据大小: {data_size}")
        else:
            self.logger.warning("无法获取数据大小")

        # 查询全文索引大小
        fulltext_index_size = "unknown"
        fulltext_index_size_sql = f"SELECT built_index_size FROM pg_catalog.hg_show_build_index_progress('{table_name}') WHERE index_name = '{table_name}_request_idx';"
        try:
            cursor.execute(fulltext_index_size_sql)
            results = cursor.fetchall()
            if results is not None and len(results) > 0 and results[0][0] is not None:
                fulltext_index_size = str(results[0][0])
                self.logger.info(f"全文索引大小: {fulltext_index_size}")
            else:
                self.logger.warning("无法获取全文索引大小")
        except Exception as e:
            self.logger.warning(f"查询全文索引大小失败: {e}")

        return load_time, vacuum_time, total_inserted, data_size, fulltext_index_size

    def _get_queries(self, table_name: str) -> Dict[str, Tuple[str, int]]:
        """从配置文件获取查询定义，并替换模板参数
        
        Args:
            table_name: 表名，用于替换模板中的 ${table_name}
            
        Returns:
            Dict[查询名称, (SQL语句, 预期结果数)]
        """
        if not self.queries_config or 'queries' not in self.queries_config:
            raise ValueError("查询配置为空或格式不正确")
        
        queries = {}
        for query_item in self.queries_config['queries']:
            name = query_item['name']

            # 检查 enable 字段，默认为 true
            if not query_item.get('enable', True):
                self.logger.info(f"跳过查询 {name}: 已禁用 (enable=false)")
                continue

            if 'hologres' not in query_item or 'sql' not in query_item['hologres'] or query_item['hologres']['sql'] is None:
                self.logger.warning(f"跳过查询 {name}: 缺少 hologres.sql 配置")
                continue
            
            sql_template = query_item['hologres']['sql'].strip()
            # 使用 string.Template 替换模板参数
            sql = Template(sql_template).safe_substitute(
                table_name=table_name
            )
            expected = query_item.get('expected_results', 0)
            queries[name] = (sql, expected)
        
        if not queries:
            raise ValueError("未找到有效的查询配置")
        
        self.logger.info(f"从配置文件加载了 {len(queries)} 个查询，表名: {table_name}")
        return queries
    
    def run_queries(self, cursor, table_name: str, repeat_times: int) -> Dict[str, Tuple[float, float, float, float, float, int, bool]]:
        queries = self._get_queries(table_name)
        
        results = {}
        
        for query_name, (sql, assert_hits) in queries.items():
            self.logger.info(f"执行查询: {query_name}")

            first_query_time = float('nan')

            query_times = []
            for i in range(repeat_times):
                start_time = time.time()
                
                cursor.execute(sql)

                if query_name == 'scroll':
                    result = []
                else:
                    result = cursor.fetchall()

                end_time = time.time()
                query_time = end_time - start_time

                if i == 0:
                    first_query_time = query_time
                    result_count = len(result)
                    result_ok = "❌" if (query_name != 'scroll' and result_count != assert_hits) else "✅"
                else:
                    query_times.append(query_time)

            total_query_time = first_query_time + sum(query_times)
            avg_query_time = sum(query_times) / len(query_times)
            min_query_time = min(query_times)
            max_query_time = max(query_times)
            results[query_name] = (total_query_time, first_query_time, min_query_time, max_query_time, avg_query_time, result_count, result_ok)

            self.logger.info(f"查询 {query_name} 完成: {result_count} 行结果, 预期 {assert_hits} 行结果, 耗时: {total_query_time:.3f}/{avg_query_time:.3f}/{max_query_time:.3f} 秒, {result_ok}")

        return results

    def run_benchmark(self, table_name: str, pipeline: str, repeat: int,
                       data_dir: str = None, batch_size: int = 10000,
                       num_parsers: int = 8) -> Dict[str, Any]:
        """运行基准测试
        
        Args:
            table_name: 表名
            pipeline: 运行模式
                - 'all': 完整流程（建表+导入+查询）
                - 'import-data-only': 只建表+导入数据
                - 'benchmark-only': 只跑查询（表已存在）
            repeat: 查询重复次数
            data_dir: NDJSON 数据文件目录 (import-data-only/all 模式必需)
            batch_size: 每批次的行数
            num_parsers: Parser 进程数
        """
        self.logger.info("=" * 50)
        self.logger.info(f"开始Hologres基准测试 (pipeline={pipeline})")
        self.logger.info("=" * 50)
        
        overall_start_time = time.time()
        
        load_time, vacuum_time, records_loaded, data_size, fulltext_index_size = 0, 0, 0, 'unknown', 'unknown'
        query_results = {}
        
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 根据pipeline模式选择性执行
            # benchmark-only 模式下先检查表是否存在，不存在则自动回退到all流程
            if pipeline == 'benchmark-only':
                cursor.execute(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_name = %s",
                    (table_name,)
                )
                row = cursor.fetchone()
                if row is None or row[0] == 0:
                    if not data_dir:
                        raise ValueError(
                            f"表 {table_name} 不存在，需要导入数据，但未指定 --data-dir"
                        )
                    pipeline = 'all'

            if pipeline in ['all', 'import-data-only']:
                # 1. 创建表
                self.create_table(cursor, table_name)
                
                # 2. 批量导入数据，构建索引
                load_time, vacuum_time, records_loaded, data_size, fulltext_index_size = self.bulk_load_data(
                    cursor, table_name, data_dir, batch_size, num_parsers)

            if pipeline in ['all', 'benchmark-only']:
                # 3. 执行查询测试
                query_results = self.run_queries(cursor, table_name, repeat)
            
        overall_end_time = time.time()
        total_time = overall_end_time - overall_start_time
        
        # 计算查询总时间
        total_query_time = sum(time for time, *_ in query_results.values()) if query_results else 0
        
        # 整理结果
        results = {
            'pipeline': pipeline,
            'load_time': round(load_time, 3),
            'vacuum_time': round(vacuum_time, 3),
            'total_query_time': round(total_query_time, 3),
            'total_time': round(total_time, 3),
            'records_loaded': records_loaded,
            'total_size': data_size,
            'index_size': fulltext_index_size,
            'repeat': repeat,
            'queries': {
                name: {
                    'time': round(time, 3),
                    'first_time': round(first_time, 3),
                    'min_time': round(min_time, 3),
                    'max_time': round(max_time, 3),
                    'avg_time': round(avg_time, 3),
                    'results': count,
                    'assert': result_ok
                }
                for name, (time, first_time, min_time, max_time, avg_time, count, result_ok) in query_results.items()
            }
        }
        
        # 输出结果
        self.print_results(results)
        
        return results

    def print_results(self, results: Dict[str, Any]):
        """打印测试结果"""
        self.logger.info("=" * 50)
        self.logger.info("基准测试结果")
        self.logger.info("=" * 50)
        
        print(f"📊 导入时间 (Load Time): {results['load_time']} 秒")
        print(f"🧹 BUILD INDEX时间: {results['vacuum_time']} 秒")
        print(f"📝 导入记录数: {results['records_loaded']:,} 条")
        print(f"📝 数据和索引总大小: {results['total_size']}")
        print(f"📝 全文索引大小: {results['index_size']}")
        print()
        
        print("🔍 查询测试结果（平均单条耗时）:")
        for query_name, query_result in results['queries'].items():
            if query_result['assert'] != '✅':
                print(f"  • {query_name}: {query_result['avg_time']} 秒 ({query_result['results']} 行结果, {query_result['assert']})")
            else:
                print(f"  • {query_name}: {query_result['avg_time']} 秒")
        
        total_first_time = sum(
            r['first_time'] for r in results['queries'].values()
        )
        total_avg_time = sum(
            r['avg_time'] for r in results['queries'].values()
        )
        repeat_count = results.get('repeat', 10)
        print(f"\n⏱️  查询总时间（各跑{repeat_count}次）: {results['total_query_time']} 秒")
        print(f"⏱️  所有查询 first_time 之和: {round(total_first_time, 3)} 秒")
        print(f"⏱️  所有查询 avg_time 之和: {round(total_avg_time, 3)} 秒")
        print(f"🎯 总体测试时间: {results['total_time']} 秒")
        
        # 打印markdown表格形式的结果，便于比较
        self.print_markdown_table(results)
        
        print("\n" + "=" * 50)

    def print_markdown_table(self, results: Dict[str, Any]):
        """打印markdown表格形式的测试结果"""
        print("\n📊 **比较用表格形式 (Markdown)**")
        print("```markdown")
        print("## Hologres 基准测试结果")
        print("")
        print("### 基本指标")
        print("| 指标 | 数值 | 单位 |")
        print("|------|------|------|")
        print(f"| 导入时间 | {results['load_time']} | 秒 |")
        print(f"| BUILD INDEX时间 | {results['vacuum_time']} | 秒 |")
        print(f"| 导入记录数 | {results['records_loaded']:,} | 条 |")
        print(f"| 数据和索引总大小 | {results['total_size']} |  |")
        print(f"| 全文索引大小 | {results['index_size']} |  |")
        print(f"| 查询总时间 | {results['total_query_time']} | 秒 |")
        print(f"| 总体测试时间 | {results['total_time']} | 秒 |")
        print("")
        print("### 查询性能细节")
        print("| 查询名称 | 总时间(秒) | 平均时间(秒) | 最大时间(秒) | 结果数 | 状态 |")
        print("|----------|------------|------------|------------|-------|-----|")
        
        # 按照平均时间排序，方便比较
        sorted_queries = sorted(
            results['queries'].items(), 
            key=lambda x: x[1]['avg_time'], 
            reverse=True
        )
        
        for query_name, query_result in sorted_queries:
            status_icon = "✅" if query_result['assert'] == '✅' else "❌"
            print(f"| {query_name} | {query_result['time']} | {query_result['avg_time']} | {query_result['max_time']} | {query_result['results']} | {status_icon} |")
        
        print("")
        print("### 性能排名 (Top 5 最慢查询)")
        print("| 排名 | 查询名称 | 平均时间(秒) |")
        print("|------|----------|------------|")
        
        top_5_slow = sorted_queries[:5]
        for i, (query_name, query_result) in enumerate(top_5_slow, 1):
            print(f"| {i} | {query_name} | {query_result['avg_time']} |")
            
        print("```")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='Hologres基准测试工具')
    
    # 配置文件参数
    parser.add_argument('--config', help='配置文件路径 (JSON格式)，指定后其他连接参数可省略')
    
    # 数据库连接参数
    parser.add_argument('--host', help='Hologres主机地址')
    parser.add_argument('--port', type=int, help='端口号 (默认: 80)')
    parser.add_argument('--database', help='数据库名')
    parser.add_argument('--username', help='用户名')
    parser.add_argument('--password', help='密码')
    
    # 测试参数
    parser.add_argument('--table', help='测试表名 (默认: http_logs)')
    parser.add_argument('--data-dir', help='NDJSON 数据文件目录 (import-data-only/all 模式必需)')
    parser.add_argument('--batch-size', type=int, default=10000, help='每批次的行数 (默认: 10000)')
    parser.add_argument('--num-parsers', type=int, default=8, help='Parser 进程数 (默认: 8)')
    parser.add_argument('--queries-config', help='查询配置文件路径 (YAML格式)')
    parser.add_argument('--output', help='结果输出到JSON文件 (默认自动生成)')
    parser.add_argument('--pipeline', choices=['all', 'import-data-only', 'benchmark-only'],
                        help='运行模式: all=完整流程, import-data-only=只导入数据, benchmark-only=只跑查询 (默认: benchmark-only)')
    parser.add_argument('--repeat', type=int, help='查询重复次数 (默认: 10)')
    
    args = parser.parse_args()
    
    # 从配置文件加载默认值
    config_defaults = {}
    if args.config:
        config_path = Path(args.config)
        if not config_path.exists():
            print(f"❌ 配置文件不存在: {args.config}")
            return 1
        with open(config_path, 'r', encoding='utf-8') as f:
            config_defaults = json.load(f)
        print(f"已加载配置文件: {args.config}")
    
    # 合并参数：命令行参数优先于配置文件参数
    def get_param(arg_value, config_key, default=None):
        if arg_value is not None:
            return arg_value
        return config_defaults.get(config_key, default)
    
    # 获取最终参数值
    host = get_param(args.host, 'host')
    port = get_param(args.port, 'port', 80)
    database = get_param(args.database, 'database')
    username = get_param(args.username, 'username')
    password = get_param(args.password, 'password')
    table = get_param(args.table, 'table_name', 'http_logs')
    data_dir = args.data_dir
    batch_size = args.batch_size
    num_parsers = args.num_parsers
    queries_config = args.queries_config
    pipeline = get_param(args.pipeline, 'pipeline', 'benchmark-only')
    repeat = get_param(args.repeat, 'repeat', 10)
    
    # 检查必需参数
    if not all([host, database, username, password]):
        print("❌ 缺少必需参数: host, database, username, password")
        print("   请通过命令行参数或 --config 配置文件提供")
        return 1
    
    # 检查 data-dir 参数 (import-data-only/all 模式必需)
    if pipeline in ['all', 'import-data-only'] and not data_dir:
        print("❌ 缺少必需参数: --data-dir")
        print("   import-data-only/all 模式需要指定 NDJSON 数据文件目录")
        return 1
    # benchmark-only 模式下 data_dir 可选，若表不存在时运行时会报错提示
    
    # 自动生成输出文件名
    output = args.output
    if not output:
        script_dir = Path(__file__).parent
        results_dir = script_dir / 'results'
        results_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output = str(results_dir / f'benchmark_results_{timestamp}.json')
    
    # 配置
    config = {
        'host': host,
        'port': port,
        'database': database,
        'username': username,
        'password': password
    }
    
    try:
        if not PSYCOPG_AVAILABLE:
            print("❌ psycopg未安装，请先运行: pip3 install psycopg[binary]")
            return 1
            
        # 运行基准测试
        benchmark = HologresBenchmark(config, queries_config)
        results = benchmark.run_benchmark(
            table, pipeline, repeat,
            data_dir=data_dir, batch_size=batch_size, num_parsers=num_parsers)
        
        # 保存结果到文件
        with open(output, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"\n📄 结果已保存到: {output}")
        
    except Exception as e:
        err = traceback.format_exc()
        print(f"❌ 基准测试失败: {str(err)}")
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())