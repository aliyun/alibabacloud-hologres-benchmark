#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Hologres Benchmark Tool"""

import time
import json
import logging
import argparse
import os
import traceback
import yaml
from typing import Dict, List, Tuple, Any
from contextlib import contextmanager
from pathlib import Path
from datetime import datetime
from string import Template

from data_loader import NDJSONImporter, FieldMapper, HologresImportConfig

try:
    import psycopg
    PSYCOPG_AVAILABLE = True
except ImportError:
    PSYCOPG_AVAILABLE = False
    print("警告: psycopg未安装，请运行: pip3 install psycopg[binary]")


class HologresBenchmark:
    """Hologres benchmark class"""

    def __init__(self, config: Dict[str, Any], queries_config_path: str = None):
        self.config = config
        self.logger = self._setup_logger()
        self.queries_config = self._load_queries_config(queries_config_path)
        self.table_name = None

    def _load_queries_config(self, config_path: str = None) -> Dict[str, Any]:
        if config_path is None:
            config_path = Path(__file__).parent / 'benchmark_queries.yaml'
        else:
            config_path = Path(config_path)

        if not config_path.exists():
            raise FileNotFoundError(f"查询配置文件不存在: {config_path}")

        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        self.logger.info(f"已加载查询配置文件: {config_path}")
        return config

    def _setup_logger(self) -> logging.Logger:
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
        if not PSYCOPG_AVAILABLE:
            raise ImportError("psycopg未安装，请运行: pip3 install psycopg[binary]")

        conn = None
        try:
            conn = psycopg.connect(
                host=self.config['host'],
                port=self.config['port'],
                dbname=self.config['database'],
                user=self.config.get('username') or None,
                password=self.config.get('password') or None,
                connect_timeout=30,
                application_name='hologres_benchmark',
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
        tg_sql = f"CALL HG_CREATE_TABLE_GROUP('tg_6', 6, 'ignore');"
        create_sql = f"""
            DROP TABLE IF EXISTS {table_name};
            PURGE TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name} (
                id BIGINT PRIMARY KEY,
                name TEXT,
                journal TEXT,
                "date" TEXT,
                volume TEXT,
                issue TEXT,
                accession TEXT,
                "timestamp" timestamptz NOT NULL,
                pmid INTEGER,
                body TEXT
            ) WITH (
                table_group = 'tg_6',
                bitmap_columns = 'journal,"date",volume,issue,accession',
                segment_key = '"timestamp"',
                clustering_key = '"timestamp"',
                distribution_key = 'id'
            );
        """

        cursor.execute(tg_sql)
        cursor.execute(create_sql)
        self.logger.info(f"已创建表: {table_name}")

        cursor.execute("set hg_experimental_enable_result_cache = off;")
        self.logger.info(f"已关闭Result Cache")

    def bulk_load_data(self, cursor, table_name: str, data_dir: str,
                        batch_size: int, num_workers: int) -> Tuple[float, float, int, str]:
        self.logger.info(f"开始导入测试数据 (data_dir={data_dir})...")
        start_time = time.time()

        try:
            if not os.path.exists(data_dir):
                self.logger.error(f"数据目录不存在: {data_dir}")
                raise Exception(f"数据目录不存在: {data_dir}")

            field_mapper = FieldMapper.from_json_fields(
                columns=["id", "name", "journal", "date", "volume", "issue", "accession", "timestamp", "pmid", "body"],
                json_fields=["id", "name", "journal", "date", "volume", "issue", "accession", "timestamp", "pmid", "body"],
                default_values={
                    "id": 0,
                    "name": "",
                    "journal": "",
                    "date": "",
                    "volume": "",
                    "issue": "",
                    "accession": "",
                    "timestamp": "1970-01-01T00:00:00Z",
                    "pmid": 0,
                    "body": ""
                }
            )

            import_config = HologresImportConfig(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                username=self.config.get('username') or '',
                password=self.config.get('password') or '',
                batch_size=batch_size,
                num_parsers=num_workers,
                show_progress=True,
                table_name=table_name
            )

            importer = NDJSONImporter(
                db_config=self.config,
                table_name=table_name,
                field_mapper=field_mapper,
                config=import_config
            )

            self.logger.info("开始加载数据...")
            import_result = importer.import_data(
                data_dir=data_dir,
                batch_size=batch_size,
                num_parsers=num_workers
            )

            insert_time = time.time() - start_time

            if not import_result.success:
                self.logger.error(f"数据导入失败: {import_result.error_messages}")
                raise Exception(f"数据导入失败: {import_result.error_messages}")

            total_inserted = import_result.records_loaded
            self.logger.info(f"数据导入完成: {total_inserted} 条记录, 耗时: {insert_time:.3f} 秒")

        except Exception as e:
            self.logger.error(f"数据导入失败: {str(e)}")
            self.logger.error(traceback.format_exc())
            raise e

        create_index_sql = f"""
            SELECT hg_admin_command('set_global_flag', 'fulltext_index_file_merge_policy=tiered');

            CREATE INDEX IF NOT EXISTS {table_name}_body_idx
            ON {table_name}
            USING FULLTEXT (body)
            WITH (tokenizer = 'standard');

            CREATE INDEX IF NOT EXISTS {table_name}_name_idx
            ON {table_name}
            USING FULLTEXT (name)
            WITH (tokenizer = 'keyword');

            CREATE INDEX IF NOT EXISTS {table_name}_journal_idx
            ON {table_name}
            USING FULLTEXT (journal)
            WITH (tokenizer = 'standard');

            CREATE INDEX IF NOT EXISTS {table_name}_date_idx
            ON {table_name}
            USING FULLTEXT ("date")
            WITH (tokenizer = 'standard');

            CREATE INDEX IF NOT EXISTS {table_name}_volume_idx
            ON {table_name}
            USING FULLTEXT (volume)
            WITH (tokenizer = 'standard');

            CREATE INDEX IF NOT EXISTS {table_name}_issue_idx
            ON {table_name}
            USING FULLTEXT (issue)
            WITH (tokenizer = 'standard');

            CREATE INDEX IF NOT EXISTS {table_name}_accession_idx
            ON {table_name}
            USING FULLTEXT (accession)
            WITH (tokenizer = 'keyword');
        """

        cursor.execute(create_index_sql)
        self.logger.info("CREATE INDEX完毕")

        cursor.execute(f"VACUUM {table_name};")
        cursor.execute(f"ANALYZE {table_name};")
        self.logger.info("VACUUM完毕")

        end_time = time.time()
        load_time = end_time - start_time
        index_time = load_time - insert_time

        self.logger.info(f"数据导入完成: {total_inserted} 条记录, 耗时: {load_time:.3f} 秒, 其中index time {index_time:.3f} 秒")

        index_size = "unknown"
        index_size_sql = f"""
            SELECT * FROM pg_size_pretty(hologres.hg_relation_size('{table_name}', 'data'));
        """
        cursor.execute(index_size_sql)
        results = cursor.fetchall()
        if results is not None and len(results) > 0:
            index_size = str(results[0][0])
            self.logger.info(f"索引大小: {index_size}")

        return load_time, index_time, total_inserted, index_size

    def execute_scroll_query(self, cursor, scroll_config: Dict[str, Any], declare_sql: str) -> List[Any]:
        pages = scroll_config.get('pages', 25)
        size = scroll_config.get('size', 100)
        cursor_name = scroll_config.get('cursor_name', 'my_cursor')

        try:
            cursor.execute("BEGIN;")
            cursor.execute(declare_sql)

            for i in range(pages):
                cursor.execute(f"FETCH {size} FROM {cursor_name};")
                batch_results = cursor.fetchall()
                if not batch_results:
                    break

            cursor.execute("COMMIT;")

        except Exception as e:
            self.logger.error(f"执行scroll查询时出错: {e}")
            try:
                cursor.execute("ROLLBACK;")
            except:
                pass
            raise

        return []

    def _render_template(self, template_str: str, variables: Dict[str, str]) -> str:
        return Template(template_str).safe_substitute(variables)

    def _get_effective_iterations(self, query_config: Dict[str, Any], config_key: str) -> int:
        defaults = self.queries_config.get('defaults', {})
        query_value = query_config.get(config_key)
        default_value = defaults.get(config_key, 100)
        return query_value if query_value is not None else default_value

    def run_queries(self, cursor, table_name: str) -> Dict[str, Tuple[float, float, float, int, str]]:
        self.table_name = table_name
        defaults = self.queries_config.get('defaults', {})
        queries = self.queries_config.get('queries', [])

        results = {}

        for query_config in queries:
            if not query_config.get('enable', True):
                continue

            query_name = query_config['name']
            hologres_query = query_config.get('hologres')

            if not hologres_query:
                self.logger.warning(f"跳过查询 {query_name}: 缺少 hologres 配置")
                continue

            self.logger.info(f"执行查询: {query_name}")

            expected_results = query_config.get('expected_results', 0)
            params = query_config.get('params', [None])
            query_type = query_config.get('type', 'default')
            scroll_config = query_config.get('scroll')
            use_cache = query_config.get('cache', False)

            warmup_iterations = self._get_effective_iterations(query_config, 'warmup-iterations')
            iterations = self._get_effective_iterations(query_config, 'iterations')

            query_times = []
            result_count = 0

            if query_type == 'scroll' and scroll_config:
                cursor_name = scroll_config.get('cursor_name', 'my_cursor')
                variables = {'table_name': table_name, 'cursor_name': cursor_name}
                declare_sql = self._render_template(hologres_query, variables)

                for i in range(warmup_iterations):
                    self.execute_scroll_query(cursor, scroll_config, declare_sql)

                for i in range(iterations):
                    start_time = time.time()
                    result = self.execute_scroll_query(cursor, scroll_config, declare_sql)
                    end_time = time.time()
                    query_times.append(end_time - start_time)
                    if i == 0:
                        result_count = 0
            else:
                if use_cache:
                    cursor.execute("SET hg_experimental_enable_result_cache = on;")

                for param in params:
                    variables = {'table_name': table_name}
                    if param:
                        variables['param'] = param

                    rendered_query = self._render_template(hologres_query, variables)

                    for _ in range(warmup_iterations):
                        try:
                            cursor.execute(rendered_query)
                            cursor.fetchall()
                        except Exception as e:
                            self.logger.warning(f"Warmup查询失败: {e}")

                    for i in range(iterations):
                        start_time = time.time()
                        try:
                            cursor.execute(rendered_query)
                            result = cursor.fetchall()
                            end_time = time.time()
                            query_times.append(end_time - start_time)
                            if i == 0 and param == params[0]:
                                result_count = len(result)
                        except Exception as e:
                            self.logger.error(f"查询执行失败: {e}")
                            end_time = time.time()
                            query_times.append(end_time - start_time)

                if use_cache:
                    cursor.execute("SET hg_experimental_enable_result_cache = off;")

            if query_times:
                sum_time = sum(query_times)
                avg_time = sum(query_times) / len(query_times)
                max_time = max(query_times)
                result_ok = "✅" if (query_name == 'scroll' or result_count == expected_results) else "❌"
                results[query_name] = (sum_time, avg_time, max_time, result_count, result_ok)
                self.logger.info(f"查询 {query_name} 完成: {result_count} 行结果, 预期 {expected_results} 行结果, 耗时: {sum_time:.3f}/{avg_time:.3f}/{max_time:.3f} 秒, {result_ok}")

        return results

    def run_benchmark(self, table_name: str, pipeline: str, repeat: int,
                       data_dir: str, batch_size: int,
                       num_workers: int) -> Dict[str, Any]:
        self.logger.info("=" * 50)
        self.logger.info(f"开始Hologres基准测试 (pipeline={pipeline})")
        self.logger.info("=" * 50)

        overall_start_time = time.time()

        load_time, vacuum_time, records_loaded, index_size = 0, 0, 0, 'unknown'
        query_results = {}

        with self.get_connection() as conn:
            cursor = conn.cursor()

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
                self.create_table(cursor, table_name)
                load_time, vacuum_time, records_loaded, index_size = self.bulk_load_data(
                    cursor, table_name, data_dir, batch_size, num_workers)

            if pipeline in ['all', 'benchmark-only']:
                query_results = self.run_queries(cursor, table_name)

        overall_end_time = time.time()
        total_time = overall_end_time - overall_start_time

        total_query_time = sum(result[0] for result in query_results.values()) if query_results else 0

        results = {
            'pipeline': pipeline,
            'load_time': round(load_time, 3),
            'vacuum_time': round(vacuum_time, 3),
            'total_query_time': round(total_query_time, 3),
            'total_time': round(total_time, 3),
            'records_loaded': records_loaded,
            'index_size': index_size,
            'queries': {
                name: {
                    'time': round(time, 3),
                    'avg_time': round(avg_time, 3),
                    'max_time': round(max_time, 3),
                    'results': count,
                    'assert': result_ok
                }
                for name, (time, avg_time, max_time, count, result_ok) in query_results.items()
            }
        }

        self.print_results(results)

        return results

    def print_results(self, results: Dict[str, Any]):
        self.logger.info("=" * 50)
        self.logger.info("基准测试结果")
        self.logger.info("=" * 50)

        print(f"导入时间 (Insert Time): {results['load_time']-results['vacuum_time']} 秒")
        print(f"BUILD INDEX时间: {results['vacuum_time']} 秒")
        print(f"数据准备总时间: {results['load_time']} 秒")
        print(f"导入记录数: {results['records_loaded']:,} 条")
        print(f"数据和索引大小: {results['index_size']}")
        print()

        print("查询测试结果（平均单条耗时）:")
        sum_avg_query_time = 0
        for query_name, query_result in results['queries'].items():
            if query_result['assert'] != '✅':
                print(f"  - {query_name}: {query_result['avg_time']} 秒 ({query_result['results']} 行结果, {query_result['assert']})")
            else:
                print(f"  - {query_name}: {query_result['avg_time']} 秒")
            sum_avg_query_time += query_result['avg_time']

        print(f"\n查询总时间: {results['total_query_time']} 秒")
        print(f"总平均时间: {sum_avg_query_time} 秒")
        print(f"总体测试时间: {results['total_time']} 秒")

        self.print_markdown_table(results)
        print("\n" + "=" * 50)

    def print_markdown_table(self, results: Dict[str, Any]):
        print("\n比较用表格形式 (Markdown)")
        print("```markdown")
        print("## Hologres 基准测试结果")
        print("")
        print("### 基本指标")
        print("| 指标 | 数值 | 单位 |")
        print("|------|------|------|")
        print(f"| 导入时间 | {results['load_time']} | 秒 |")
        print(f"| BUILD INDEX时间 | {results['vacuum_time']} | 秒 |")
        print(f"| 导入记录数 | {results['records_loaded']:,} | 条 |")
        print(f"| 查询总时间 | {results['total_query_time']} | 秒 |")
        print(f"| 总体测试时间 | {results['total_time']} | 秒 |")
        print("")
        print("### 查询性能细节")
        print("| 查询名称 | 总时间(秒) | 平均时间(秒) | 最大时间(秒) | 结果数 | 状态 |")
        print("|----------|------------|------------|------------|-------|-----|")

        sorted_queries = sorted(
            results['queries'].items(),
            key=lambda x: x[1]['avg_time'],
            reverse=True
        )

        for query_name, query_result in sorted_queries:
            status_icon = "OK" if query_result['assert'] == '✅' else "FAIL"
            print(f"| {query_name} | {query_result['time']} | {query_result['avg_time']} | {query_result['max_time']} | {query_result['results']} | {status_icon} |")

        print("```")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='Hologres基准测试工具')

    parser.add_argument('--config', help='数据库配置文件路径')
    parser.add_argument('--host', help='Hologres主机地址')
    parser.add_argument('--port', type=int, help='端口号 (默认: 80)')
    parser.add_argument('--database', help='数据库名')
    parser.add_argument('--username', help='用户名')
    parser.add_argument('--password', help='密码')

    parser.add_argument('--table', help='测试表名 (默认: pmc)')
    parser.add_argument('--data-dir', help='数据文件目录')
    parser.add_argument('--batch-size', type=int, default=1000, help='每批次的行数 (默认: 1000)')
    parser.add_argument('--num-workers', type=int, default=8, help='Worker 进程数 (默认: 8)')
    parser.add_argument('--output', help='结果输出到JSON文件 (默认自动生成)')
    parser.add_argument('--pipeline', choices=['all', 'import-data-only', 'benchmark-only'],
                        help='运行模式: all=完整流程, import-data-only=只导入数据, benchmark-only=只跑查询')
    parser.add_argument('--queries-config', help='查询配置文件路径')

    args = parser.parse_args()

    config_defaults = {}
    if args.config:
        config_path = Path(args.config)
        if not config_path.exists():
            print(f"配置文件不存在: {args.config}")
            return 1
        with open(config_path, 'r', encoding='utf-8') as f:
            config_defaults = json.load(f)
        print(f"已加载配置文件: {args.config}")

    def get_param(arg_value, config_key, default=None):
        if arg_value is not None:
            return arg_value
        return config_defaults.get(config_key, default)

    host = get_param(args.host, 'host')
    port = get_param(args.port, 'port', 80)
    database = get_param(args.database, 'database')
    username = get_param(args.username, 'username')
    password = get_param(args.password, 'password')
    table = get_param(args.table, 'table_name', 'pmc')
    data_dir = args.data_dir
    batch_size = args.batch_size
    num_workers = args.num_workers
    pipeline = args.pipeline if args.pipeline else 'benchmark-only'

    if not all([host, database, username, password]):
        print("缺少必需参数: host, database, username, password")
        return 1

    if pipeline in ['all', 'import-data-only'] and not data_dir:
        print("缺少必需参数: --data-dir")
        return 1

    output = args.output
    if not output:
        script_dir = Path(__file__).parent
        results_dir = script_dir / 'results'
        results_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output = str(results_dir / f'benchmark_results_{timestamp}.json')

    config = {
        'host': host,
        'port': port,
        'database': database,
        'username': username,
        'password': password
    }

    try:
        if not PSYCOPG_AVAILABLE:
            print("psycopg未安装，请先运行: pip3 install psycopg[binary]")
            return 1

        benchmark = HologresBenchmark(config, args.queries_config)
        results = benchmark.run_benchmark(
            table, pipeline, 0,
            data_dir=data_dir, batch_size=batch_size, num_workers=num_workers)

        with open(output, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"\n结果已保存到: {output}")

    except Exception as e:
        err = traceback.format_exc()
        print(f"基准测试失败: {str(err)}")
        return 1

    return 0


if __name__ == '__main__':
    exit(main())
