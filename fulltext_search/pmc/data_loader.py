#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""NDJSON Data Importer for Hologres"""

from multiprocessing import Process, Queue, Value
import json
import csv
import io
import time
from pathlib import Path
from typing import Callable, Dict, Any, List, Optional, Tuple, Union
from queue import Empty, Full
from dataclasses import dataclass, field

import setproctitle

try:
    import psycopg
    PSYCOPG_AVAILABLE = True
except ImportError:
    PSYCOPG_AVAILABLE = False
    print("Warning: psycopg not installed, please run: pip install psycopg[binary]")


# =============================================================================
# Configuration Classes
# =============================================================================

@dataclass
class ImportConfig:
    """Base import configuration class"""
    batch_size: int = 1000
    workers: int = 4
    show_progress: bool = True
    file_pattern: str = "*.json"
    timeout: int = 300
    on_batch_complete: Optional[Callable[[int, int], None]] = None
    on_error: Optional[Callable[[str, Exception], None]] = None


@dataclass
class ImportResult:
    """Import result class"""
    success: bool = False
    load_time: float = 0.0
    records_loaded: int = 0
    batches: int = 0
    errors: int = 0
    total_files: int = 0
    total_lines: int = 0
    error_messages: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'success': self.success,
            'load_time': round(self.load_time, 3),
            'records_loaded': self.records_loaded,
            'batches': self.batches,
            'errors': self.errors,
            'total_files': self.total_files,
            'total_lines': self.total_lines,
            'error_messages': self.error_messages[:5]
        }


@dataclass
class HologresImportConfig(ImportConfig):
    """Hologres import configuration"""
    host: str = "localhost"
    port: int = 80
    database: str = "postgres"
    username: str = "postgres"
    password: str = ""
    table_name: str = "default_table"
    queue_size: int = 32
    num_parsers: int = 8


# =============================================================================
# Field Mapper
# =============================================================================

class FieldMapper:
    """Field mapper for NDJSON to database columns"""

    def __init__(self, columns: List[str],
                 field_extractor: Optional[Callable[[int, Dict[str, Any]], Tuple]] = None):
        self.columns = columns
        self.field_extractor = field_extractor

    def extract(self, row_id: int, data: Dict[str, Any]) -> Tuple:
        if self.field_extractor:
            return self.field_extractor(row_id, data)
        return tuple(data.get(col, None) for col in self.columns)

    @classmethod
    def from_json_fields(cls, columns: List[str],
                         json_fields: Optional[List[str]] = None,
                         default_values: Optional[Dict[str, Any]] = None) -> 'FieldMapper':
        json_fields = json_fields or columns
        default_values = default_values or {}

        def extractor(row_id: int, data: Dict[str, Any]) -> Tuple:
            values = [row_id] if "id" in columns else []
            for i, fld in enumerate(json_fields):
                if fld == "id" and "id" in columns:
                    continue
                values.append(data.get(fld, default_values.get(fld)))
            return tuple(values)

        return cls(columns=columns, field_extractor=extractor)


# =============================================================================
# Worker Functions
# =============================================================================

def _reader_worker(data_dir: str, batch_size: int, queue_raws: List[Queue],
                   num_parsers: int, total_lines: Value, total_files: Value,
                   file_pattern: str = "*.json"):
    setproctitle.setproctitle('hg-ndjson:Reader')

    batch = []
    line_count = 0
    file_count = 0
    current_id = 1
    parser_index = 0

    data_path = Path(data_dir)
    json_files = sorted(data_path.glob(file_pattern))

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
                   parsed_count: Value, error_count: Value,
                   field_mapper: FieldMapper):
    setproctitle.setproctitle(f'hg-ndjson:Parser-{parser_id}')

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
                row = field_mapper.extract(row_id, data)
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
                     columns: List[str], total_batches: Value):
    setproctitle.setproctitle('hg-ndjson:Inserter')

    if not PSYCOPG_AVAILABLE:
        print("Error: psycopg not installed")
        return

    conninfo = (
        f"host={db_config['host']} "
        f"port={db_config['port']} "
        f"dbname={db_config['database']} "
        f"user={db_config['username']} "
        f"password={db_config['password']}"
    )

    batch_count = 0
    active_queues = set(range(num_parsers))

    try:
        with psycopg.connect(conninfo, application_name='hologres_benchmark') as conn:
            with conn.cursor() as cur:
                # 使用 COPY 模式
                columns_str = ', '.join(columns)
                with cur.copy(f"COPY {table_name} ({columns_str}) FROM STDIN WITH (FORMAT CSV)") as copy:
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


# =============================================================================
# Main Importer Class
# =============================================================================

class NDJSONImporter:
    """NDJSON Data Importer for Hologres"""

    def __init__(self, db_config: Dict[str, Any], table_name: str,
                 field_mapper: Optional[FieldMapper] = None,
                 columns: Optional[List[str]] = None,
                 config: Optional[ImportConfig] = None):
        if config is None:
            config = HologresImportConfig(**db_config)
        self.config = config
        self.db_config = db_config
        self.table_name = table_name
        self.field_mapper = field_mapper
        self.columns = columns or (field_mapper.columns if field_mapper else [])

        if not self.columns:
            raise ValueError("必须提供 columns 或 field_mapper")

        if self.field_mapper is None:
            self.field_mapper = FieldMapper.from_json_fields(columns=self.columns)

    def import_data(self, data_dir: Union[str, Path],
                    batch_size: Optional[int] = None,
                    num_parsers: Optional[int] = None) -> ImportResult:
        setproctitle.setproctitle('hg-ndjson:Main')

        batch_size = batch_size or self.config.batch_size
        num_parsers = num_parsers or getattr(self.config, 'num_parsers', self.config.workers)

        data_path = Path(data_dir).expanduser()

        if not data_path.exists():
            return ImportResult(
                success=False,
                error_messages=[f"数据目录不存在: {data_dir}"]
            )

        json_files = list(data_path.glob(self.config.file_pattern))
        if not json_files:
            return ImportResult(
                success=False,
                error_messages=[f"数据目录中没有 {self.config.file_pattern} 文件: {data_dir}"]
            )

        if self.config.show_progress:
            print(f"找到 {len(json_files)} 个数据文件，启动导入...")
            print(f"Parser 进程数: {num_parsers}, 批次大小: {batch_size}")

        queue_size = getattr(self.config, 'queue_size', 32)
        queue_raws = [Queue(maxsize=queue_size) for _ in range(num_parsers)]
        queue_csvs = [Queue(maxsize=queue_size) for _ in range(num_parsers)]

        total_lines = Value('q', 0)
        total_files = Value('i', 0)
        parsed_count = Value('q', 0)
        error_count = Value('q', 0)
        total_batches = Value('i', 0)

        procs = []

        reader_proc = Process(
            target=_reader_worker,
            args=(str(data_path), batch_size, queue_raws,
                  num_parsers, total_lines, total_files, self.config.file_pattern),
            name='Reader'
        )
        reader_proc.start()
        procs.append(reader_proc)

        parser_procs = []
        for i in range(num_parsers):
            proc = Process(
                target=_parser_worker,
                args=(queue_raws[i], queue_csvs[i], i, parsed_count, error_count,
                      self.field_mapper),
                name=f'Parser-{i}'
            )
            proc.start()
            procs.append(proc)
            parser_procs.append(proc)

        inserter_proc = Process(
            target=_inserter_worker,
            args=(queue_csvs, num_parsers, self.db_config, self.table_name,
                  self.columns, total_batches),
            name='Inserter'
        )
        inserter_proc.start()
        procs.append(inserter_proc)

        start_time = time.time()

        reader_proc.join()
        for proc in parser_procs:
            proc.join()
        inserter_proc.join()

        load_time = time.time() - start_time

        return ImportResult(
            success=True,
            load_time=load_time,
            records_loaded=parsed_count.value,
            batches=total_batches.value,
            errors=error_count.value,
            total_files=total_files.value,
            total_lines=total_lines.value
        )

    def test_connection(self) -> bool:
        if not PSYCOPG_AVAILABLE:
            print("Error: psycopg not installed")
            return False

        conninfo = (
            f"host={self.db_config['host']} "
            f"port={self.db_config['port']} "
            f"dbname={self.db_config['database']} "
            f"user={self.db_config['username']} "
            f"password={self.db_config['password']}"
        )

        try:
            with psycopg.connect(conninfo, application_name='hologres_benchmark') as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    return True
        except Exception as e:
            print(f"数据库连接失败: {e}")
            return False

    def get_stats(self) -> Dict[str, Any]:
        if not PSYCOPG_AVAILABLE:
            return {'doc_count': 0, 'size_in_bytes': 0, 'exists': False}

        conninfo = (
            f"host={self.db_config['host']} "
            f"port={self.db_config['port']} "
            f"dbname={self.db_config['database']} "
            f"user={self.db_config['username']} "
            f"password={self.db_config['password']}"
        )

        try:
            with psycopg.connect(conninfo, application_name='hologres_benchmark') as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables 
                            WHERE table_name = %s
                        )
                    """, (self.table_name,))
                    exists = cur.fetchone()[0]
                    
                    if not exists:
                        return {'doc_count': 0, 'size_in_bytes': 0, 'exists': False}
                    
                    cur.execute(f"SELECT COUNT(*) FROM {self.table_name}")
                    count = cur.fetchone()[0]
                    
                    return {
                        'doc_count': count,
                        'size_in_bytes': 0,
                        'exists': True
                    }
        except Exception as e:
            print(f"获取统计信息失败: {e}")
            return {'doc_count': 0, 'size_in_bytes': 0, 'exists': False}


# =============================================================================
# Convenience Function
# =============================================================================

def import_ndjson_to_hologres(
        data_dir: Union[str, Path],
        db_config: Dict[str, Any],
        table_name: str,
        columns: List[str],
        field_mapper: Optional[Union[FieldMapper, Callable]] = None,
        batch_size: int = 10000,
        num_parsers: int = 8,
        show_progress: bool = True,
        **kwargs) -> Dict[str, Any]:
    """Import data from NDJSON directory to Hologres"""
    mapper = None
    if field_mapper is not None:
        if callable(field_mapper) and not isinstance(field_mapper, FieldMapper):
            mapper = FieldMapper(columns=columns, field_extractor=field_mapper)
        else:
            mapper = field_mapper

    config = HologresImportConfig(
        **db_config,
        table_name=table_name,
        batch_size=batch_size,
        num_parsers=num_parsers,
        show_progress=show_progress,
        **kwargs
    )

    importer = NDJSONImporter(
        db_config=db_config,
        table_name=table_name,
        field_mapper=mapper,
        columns=columns,
        config=config
    )

    result = importer.import_data(data_dir=data_dir)
    return result.to_dict()


# =============================================================================
# Exports
# =============================================================================

HologresDataImporter = NDJSONImporter

__all__ = [
    'NDJSONImporter',
    'HologresDataImporter',
    'FieldMapper',
    'HologresImportConfig',
    'ImportResult',
    'import_ndjson_to_hologres',
]
