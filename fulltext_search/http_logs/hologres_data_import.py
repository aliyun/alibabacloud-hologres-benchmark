#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hologres 数据导入模块
"""

import multiprocessing as mp
from multiprocessing import Process, Queue, Value
import json
import csv
import io
import time
from pathlib import Path
from typing import Dict, Any, List
from queue import Empty, Full

import setproctitle
import psycopg


def _reader_worker(data_dir: str, batch_size: int, queue_raws: List[Queue],
                   num_parsers: int, total_lines: Value, total_files: Value):
    """
    Reader Worker - 单进程
    读取数据文件夹中的所有 .json 文件，为每行分配 ID，轮询放入各 Parser 的 queue_raw
    """
    setproctitle.setproctitle('hg-import:Reader')

    batch = []
    line_count = 0
    file_count = 0
    current_id = 1
    parser_index = 0

    data_path = Path(data_dir)
    json_files = sorted(data_path.glob("*.json"))

    for json_file in json_files:
        file_count += 1

        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        batch.append((current_id, line))
                        current_id += 1
                        line_count += 1

                        if len(batch) >= batch_size:
                            # 轮询放入各 Parser 的队列
                            for _ in range(num_parsers):
                                try:
                                    queue_raws[parser_index].put_nowait(batch)
                                    parser_index = (parser_index + 1) % num_parsers
                                    break
                                except Full:
                                    parser_index = (parser_index + 1) % num_parsers
                            else:
                                queue_raws[parser_index].put(batch)
                                parser_index = (parser_index + 1) % num_parsers
                            batch = []
        except Exception:
            continue

    # 处理最后一批
    if batch:
        queue_raws[parser_index].put(batch)

    # 更新统计信息
    total_lines.value = line_count
    total_files.value = file_count

    # 发送终止信号
    for q in queue_raws:
        q.put(None)


def _parser_worker(queue_raw: Queue, queue_csv: Queue, parser_id: int,
                   parsed_count: Value, error_count: Value):
    """
    Parser Worker - 多进程
    从 queue_raw 读取数据，解析 JSON 转换为 CSV 格式字符串，放入 queue_csv
    """
    setproctitle.setproctitle(f'hg-import:Parser-{parser_id}')

    local_parsed = 0
    local_errors = 0

    while True:
        batch = queue_raw.get()

        if batch is None:
            queue_csv.put(None)
            break

        output = io.StringIO()
        writer = csv.writer(output, delimiter=',', quoting=csv.QUOTE_MINIMAL)

        for row_id, line in batch:
            try:
                data = json.loads(line)
                row = (
                    row_id,
                    data.get('@timestamp'),
                    data.get('clientip'),
                    data.get('request'),
                    data.get('status'),
                    data.get('size')
                )
                writer.writerow(row)
                local_parsed += 1
            except (json.JSONDecodeError, Exception):
                local_errors += 1

        csv_str = output.getvalue()
        if csv_str:
            queue_csv.put(csv_str)

    # 更新全局计数
    with parsed_count.get_lock():
        parsed_count.value += local_parsed
    with error_count.get_lock():
        error_count.value += local_errors


def _inserter_worker(queue_csvs: List[Queue], num_parsers: int,
                     db_config: Dict[str, Any], table_name: str,
                     total_batches: Value):
    """
    Inserter Worker - 单进程
    使用 psycopg3 cursor.copy 长事务模式导入数据库
    """
    setproctitle.setproctitle('hg-import:Inserter')

    conninfo = (
        f"host={db_config['host']} "
        f"port={db_config['port']} "
        f"dbname={db_config['database']} "
        f"user={db_config['username']} "
        f"password={db_config['password']} "
        f"application_name=hologres_benchmark"
    )

    batch_count = 0
    active_queues = set(range(num_parsers))

    try:
        with psycopg.connect(conninfo) as conn:
            with conn.cursor() as cur:
                with cur.copy(f"COPY {table_name} FROM STDIN WITH (FORMAT CSV)") as copy:
                    while active_queues:
                        got_data = False

                        for i in list(active_queues):
                            try:
                                csv_str = queue_csvs[i].get_nowait()
                                if csv_str is None:
                                    active_queues.remove(i)
                                else:
                                    copy.write(csv_str)
                                    batch_count += 1
                                    got_data = True
                            except Empty:
                                continue

                        if not got_data and active_queues:
                            time.sleep(0.001)

            conn.commit()

    except Exception as e:
        print(f"数据库操作失败: {e}")

    total_batches.value = batch_count


class HologresDataImporter:
    """Hologres 数据导入器"""

    def __init__(self, config: Dict[str, Any], table_name: str = "http_logs"):
        """初始化

        Args:
            config: 数据库连接配置 (host, port, database, username, password)
            table_name: 目标表名
        """
        self.config = config
        self.table_name = table_name

    def import_data(self, data_dir: str, batch_size: int = 10000,
                    num_parsers: int = 8) -> Dict[str, Any]:
        """执行数据导入

        Args:
            data_dir: NDJSON 数据文件目录
            batch_size: 每批次的行数
            num_parsers: Parser 进程数

        Returns:
            导入结果统计:
            {
                'success': bool,
                'load_time': float,       # 导入耗时(秒)
                'records_loaded': int,    # 导入记录数
                'batches': int,           # 处理批次数
                'errors': int             # 错误数
            }
        """
        setproctitle.setproctitle('hg-import:Main')

        data_path = Path(data_dir).expanduser()

        if not data_path.exists():
            return {'success': False, 'error': f'数据目录不存在: {data_dir}'}

        json_files = list(data_path.glob("*.json"))
        if not json_files:
            return {'success': False, 'error': f'数据目录中没有 json 文件: {data_dir}'}

        # 创建队列
        queue_raws = [Queue(maxsize=32) for _ in range(num_parsers)]
        queue_csvs = [Queue(maxsize=32) for _ in range(num_parsers)]

        # 创建共享变量
        total_lines = Value('q', 0)
        total_files = Value('i', 0)
        parsed_count = Value('q', 0)
        error_count = Value('q', 0)
        total_batches = Value('i', 0)

        # 启动所有进程
        procs = []

        # 启动 Reader
        reader_proc = Process(
            target=_reader_worker,
            args=(str(data_path), batch_size, queue_raws,
                  num_parsers, total_lines, total_files),
            name='Reader'
        )
        reader_proc.start()
        procs.append(reader_proc)

        # 启动 Parser
        parser_procs = []
        for i in range(num_parsers):
            proc = Process(
                target=_parser_worker,
                args=(queue_raws[i], queue_csvs[i], i, parsed_count, error_count),
                name=f'Parser-{i}'
            )
            proc.start()
            procs.append(proc)
            parser_procs.append(proc)

        # 启动 Inserter
        inserter_proc = Process(
            target=_inserter_worker,
            args=(queue_csvs, num_parsers, self.config, self.table_name, total_batches),
            name='Inserter'
        )
        inserter_proc.start()
        procs.append(inserter_proc)

        # 记录开始时间
        start_time = time.time()

        # 等待所有进程结束
        reader_proc.join()
        for proc in parser_procs:
            proc.join()
        inserter_proc.join()

        # 计算耗时
        load_time = time.time() - start_time

        return {
            'success': True,
            'load_time': round(load_time, 3),
            'records_loaded': parsed_count.value,
            'batches': total_batches.value,
            'errors': error_count.value
        }


if __name__ == '__main__':
    mp.set_start_method('spawn', force=True)

    import argparse

    parser = argparse.ArgumentParser(description='Hologres 数据导入工具')
    parser.add_argument('--config', required=True, help='数据库配置文件路径 (JSON格式)')
    parser.add_argument('--data-dir', required=True, help='NDJSON 数据文件夹路径')
    parser.add_argument('--table', default='http_logs', help='目标表名 (默认: http_logs)')
    parser.add_argument('--batch-size', type=int, default=10000, help='每批次的行数 (默认: 10000)')
    parser.add_argument('--num-parsers', type=int, default=8, help='Parser 进程数 (默认: 8)')

    args = parser.parse_args()

    # 加载配置文件
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"错误: 配置文件不存在: {args.config}")
        exit(1)

    with open(config_path, 'r', encoding='utf-8') as f:
        db_config = json.load(f)

    # 执行导入
    importer = HologresDataImporter(db_config, args.table)
    result = importer.import_data(
        data_dir=args.data_dir,
        batch_size=args.batch_size,
        num_parsers=args.num_parsers
    )

    if result['success']:
        print(f"导入完成: {result['records_loaded']:,} 条记录, "
              f"耗时 {result['load_time']:.3f} 秒, "
              f"错误 {result['errors']} 条")
    else:
        print(f"导入失败: {result.get('error', '未知错误')}")
        exit(1)
