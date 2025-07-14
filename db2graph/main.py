#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据库图构建主程序（多线程版本）
从spider2-snow.jsonl文件中提取所有数据库ID并构建对应的知识图谱
支持多线程并行构建以提高效率

多线程特性：
- 每个线程使用独立的GraphBuilder实例，避免数据竞争
- 线程安全的进度跟踪和统计信息收集
- 支持用户中断和优雅关闭
- 提供并行性能分析（加速比、效率等）

注意事项：
- 多线程模式下使用 --clear 可能导致数据竞争，建议先清理再构建
- 推荐线程数：2-8，具体取决于系统性能和数据库服务器负载
- Neo4j数据库需要支持并发连接

使用建议：
- 小量数据库（<10个）：单线程模式
- 中量数据库（10-50个）：2-4线程
- 大量数据库（>50个）：4-8线程
"""

import sys
import os
import json
import logging
import argparse
from typing import List, Set, Dict, Any
import time
from collections import defaultdict
from tqdm import tqdm
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue
from threading import Lock

# 添加项目路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db2graph.GraphBuild import GraphBuilder

# 配置日志
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('db2graph_build.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


class DatabaseGraphManager:
    """数据库图构建管理器（支持多线程）"""
    
    def __init__(self, jsonl_file: str = "spider2-snow.jsonl"):
        """
        初始化管理器
        
        Args:
            jsonl_file: spider2-snow.jsonl文件路径
        """
        self.jsonl_file = jsonl_file
        self.builder = GraphBuilder()  # 主线程的builder，用于验证连接和目录操作
        self.build_stats = defaultdict(list)  # 记录构建统计信息
        self.stats_lock = Lock()  # 保护统计信息的线程锁
        self.progress_lock = Lock()  # 保护进度更新的线程锁
        
    def extract_database_ids(self) -> List[str]:
        """
        从spider2-snow.jsonl文件中提取所有唯一的数据库ID
        
        Returns:
            排序后的数据库ID列表
        """
        logger.info(f"正在读取文件: {self.jsonl_file}")
        
        if not os.path.exists(self.jsonl_file):
            logger.error(f"文件不存在: {self.jsonl_file}")
            return []
        
        db_ids = set()
        
        try:
            with open(self.jsonl_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        data = json.loads(line)
                        if 'db_id' in data:
                            db_ids.add(data['db_id'])
                    except json.JSONDecodeError as e:
                        logger.warning(f"第 {line_num} 行JSON解析失败: {e}")
                        continue
            
            db_ids_sorted = sorted(list(db_ids))
            logger.info(f"成功提取 {len(db_ids_sorted)} 个唯一数据库ID")
            
            # 显示数据库列表
            logger.info("发现的数据库列表:")
            for i, db_id in enumerate(db_ids_sorted, 1):
                logger.info(f"  {i:3d}. {db_id}")
            
            return db_ids_sorted
            
        except Exception as e:
            logger.error(f"读取文件失败: {e}")
            return []
    
    def verify_database_directories(self, db_ids: List[str]) -> List[str]:
        """
        验证数据库目录是否存在
        
        Args:
            db_ids: 数据库ID列表
            
        Returns:
            存在对应目录的数据库ID列表
        """
        logger.info("正在验证数据库目录...")
        
        available_dbs = []
        missing_dbs = []
        
        database_root = self.builder.database_root
        
        for db_id in db_ids:
            db_path = os.path.join(database_root, db_id)
            if os.path.exists(db_path) and os.path.isdir(db_path):
                available_dbs.append(db_id)
                logger.debug(f"  ✓ {db_id} - 目录存在")
            else:
                missing_dbs.append(db_id)
                logger.warning(f"  ✗ {db_id} - 目录不存在: {db_path}")
        
        logger.info(f"目录验证完成: {len(available_dbs)} 个可用, {len(missing_dbs)} 个缺失")
        
        if missing_dbs:
            logger.info("缺失的数据库:")
            for db_id in missing_dbs:
                logger.info(f"  - {db_id}")
        
        return available_dbs
    
    def _build_database_worker(self, db_id: str, thread_id: int, clear_before: bool = False, 
                              show_stats: bool = False) -> Dict[str, Any]:
        """
        线程工作函数：构建单个数据库
        
        Args:
            db_id: 数据库ID
            thread_id: 线程ID
            clear_before: 是否在构建前清理现有数据
            show_stats: 是否显示统计信息
            
        Returns:
            构建结果字典
        """
        # 为每个线程创建独立的GraphBuilder实例
        thread_builder = GraphBuilder()
        result = {
            'db_id': db_id,
            'thread_id': thread_id,
            'success': False,
            'build_time': 0,
            'error': None
        }
        
        # 创建线程专用的日志器
        thread_logger = logging.getLogger(f"{__name__}.thread_{thread_id}")
        
        try:
            thread_logger.info(f"[线程 {thread_id}] 开始构建数据库: {db_id}")
            start_time = time.time()
            
            # 可选清理现有数据（注意：多线程环境下要小心）
            if clear_before:
                thread_logger.info(f"[线程 {thread_id}] 清理现有图数据...")
                if not thread_builder.clear_existing_graph():
                    raise Exception("清理图数据失败")
            
            # 构建图
            success = thread_builder.build_database_graph(db_id)
            
            end_time = time.time()
            build_time = end_time - start_time
            result['build_time'] = build_time
            
            if success:
                thread_logger.info(f"[线程 {thread_id}] ✓ 数据库 {db_id} 构建成功! (耗时: {build_time:.2f}s)")
                result['success'] = True
                
                # 线程安全地记录统计信息
                with self.stats_lock:
                    self.build_stats['success'].append({
                        'db_id': db_id,
                        'thread_id': thread_id,
                        'build_time': build_time
                    })
                
                # 可选显示统计信息
                if show_stats:
                    thread_logger.info(f"[线程 {thread_id}] 构建统计信息:")
                    thread_builder.validator.print_graph_summary()
            else:
                error_msg = f"构建失败"
                thread_logger.error(f"[线程 {thread_id}] ✗ 数据库 {db_id} 构建失败! (耗时: {build_time:.2f}s)")
                result['error'] = error_msg
                
                with self.stats_lock:
                    self.build_stats['failed'].append({
                        'db_id': db_id,
                        'thread_id': thread_id,
                        'build_time': build_time,
                        'error': error_msg
                    })
                
        except Exception as e:
            end_time = time.time()
            build_time = end_time - start_time
            result['build_time'] = build_time
            result['error'] = str(e)
            
            thread_logger.error(f"[线程 {thread_id}] ✗ 数据库 {db_id} 构建过程中发生异常: {e}")
            
            with self.stats_lock:
                self.build_stats['failed'].append({
                    'db_id': db_id,
                    'thread_id': thread_id,
                    'build_time': build_time,
                    'error': str(e)
                })
        finally:
            # 确保线程的GraphBuilder实例被正确关闭
            try:
                thread_builder.close()
            except:
                pass
        
        return result
    
    def build_single_database(self, db_id: str, clear_before: bool = False, 
                             show_stats: bool = False) -> bool:
        """
        构建单个数据库的图
        
        Args:
            db_id: 数据库ID
            clear_before: 是否在构建前清理现有数据
            show_stats: 是否显示统计信息
            
        Returns:
            是否构建成功
        """
        logger.info("=" * 80)
        logger.info(f"开始构建数据库: {db_id}")
        logger.info("=" * 80)
        
        start_time = time.time()
        
        try:
            # 可选清理现有数据
            if clear_before:
                logger.info("清理现有图数据...")
                if not self.builder.clear_existing_graph():
                    logger.error("清理图数据失败")
                    return False
            
            # 构建图
            success = self.builder.build_database_graph(db_id)
            
            end_time = time.time()
            build_time = end_time - start_time
            
            if success:
                logger.info(f"✓ 数据库 {db_id} 构建成功! (耗时: {build_time:.2f}s)")
                
                # 记录统计信息
                self.build_stats['success'].append({
                    'db_id': db_id,
                    'build_time': build_time
                })
                
                # 可选显示统计信息
                if show_stats:
                    logger.info("构建统计信息:")
                    self.builder.validator.print_graph_summary()
                    
                return True
            else:
                logger.error(f"✗ 数据库 {db_id} 构建失败! (耗时: {build_time:.2f}s)")
                self.build_stats['failed'].append({
                    'db_id': db_id,
                    'build_time': build_time,
                    'error': '构建失败'
                })
                return False
                
        except Exception as e:
            end_time = time.time()
            build_time = end_time - start_time
            
            logger.error(f"✗ 数据库 {db_id} 构建过程中发生异常: {e}")
            logging.exception(f"详细错误信息 ({db_id}):")
            
            self.build_stats['failed'].append({
                'db_id': db_id,
                'build_time': build_time,
                'error': str(e)
            })
            return False
    
    def build_all_databases_parallel(self, db_ids: List[str], max_workers: int = 4,
                                    clear_before_each: bool = False, show_stats_each: bool = False, 
                                    continue_on_error: bool = True) -> dict:
        """
        多线程并行构建所有数据库的图
        
        Args:
            db_ids: 要构建的数据库ID列表
            max_workers: 最大线程数
            clear_before_each: 是否在每个数据库构建前清理数据
            show_stats_each: 是否为每个数据库显示统计信息
            continue_on_error: 是否在出错时继续构建其他数据库
            
        Returns:
            构建结果统计
        """
        logger.info("*" * 100)
        logger.info(f"开始多线程批量构建 {len(db_ids)} 个数据库 (最大 {max_workers} 个线程)")
        logger.info("*" * 100)
        
        if clear_before_each:
            logger.warning("⚠️  多线程模式下启用 --clear 可能导致数据竞争，建议单独清理图数据")
        
        overall_start_time = time.time()
        completed_count = 0
        
        # 使用线程安全的进度条
        with tqdm(total=len(db_ids), desc="构建数据库", unit="db", 
                  bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}") as pbar:
            
            # 使用ThreadPoolExecutor进行并行处理
            with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="DBBuilder") as executor:
                # 提交所有任务
                future_to_db = {}
                for i, db_id in enumerate(db_ids):
                    future = executor.submit(
                        self._build_database_worker,
                        db_id, 
                        i + 1,  # thread_id
                        clear_before_each,
                        show_stats_each
                    )
                    future_to_db[future] = db_id
                
                # 处理完成的任务
                try:
                    for future in as_completed(future_to_db):
                        db_id = future_to_db[future]
                        completed_count += 1
                        
                        try:
                            result = future.result()
                            
                            # 线程安全地更新进度条
                            with self.progress_lock:
                                success_count = len(self.build_stats['success'])
                                failed_count = len(self.build_stats['failed'])
                                
                                pbar.set_postfix({
                                    '完成': f"{completed_count}/{len(db_ids)}",
                                    '成功': success_count,
                                    '失败': failed_count,
                                    '当前': result['db_id']
                                })
                                pbar.update(1)
                            
                            if result['success']:
                                logger.info(f"✓ [{completed_count}/{len(db_ids)}] {result['db_id']} 完成 "
                                          f"(线程 {result['thread_id']}, 耗时: {result['build_time']:.2f}s)")
                            else:
                                logger.error(f"✗ [{completed_count}/{len(db_ids)}] {result['db_id']} 失败 "
                                           f"(线程 {result['thread_id']}, 耗时: {result['build_time']:.2f}s) "
                                           f"错误: {result['error']}")
                                
                                if not continue_on_error:
                                    logger.error("遇到错误且设置了 --stop-on-error，取消剩余任务")
                                    # 取消剩余的任务
                                    for remaining_future in future_to_db:
                                        if not remaining_future.done():
                                            remaining_future.cancel()
                                    break
                            
                        except Exception as e:
                            logger.error(f"处理数据库 {db_id} 的结果时发生错误: {e}")
                            if not continue_on_error:
                                break
                                
                except KeyboardInterrupt:
                    logger.info("用户中断，正在取消剩余任务...")
                    # 取消所有未完成的任务
                    for future in future_to_db:
                        if not future.done():
                            future.cancel()
                    
                    # 等待正在运行的任务完成
                    executor.shutdown(wait=True)
                    logger.info("所有线程已停止")
        
        overall_end_time = time.time()
        overall_time = overall_end_time - overall_start_time
        
        # 生成最终统计报告
        self.print_final_report(overall_time, max_workers)
        
        return {
            'total_time': overall_time,
            'success_count': len(self.build_stats['success']),
            'failed_count': len(self.build_stats['failed']),
            'success_list': [item['db_id'] for item in self.build_stats['success']],
            'failed_list': [item['db_id'] for item in self.build_stats['failed']],
            'max_workers': max_workers
        }
    
    def build_all_databases(self, db_ids: List[str], clear_before_each: bool = False,
                           show_stats_each: bool = False, continue_on_error: bool = True) -> dict:
        """
        构建所有数据库的图
        
        Args:
            db_ids: 要构建的数据库ID列表
            clear_before_each: 是否在每个数据库构建前清理数据
            show_stats_each: 是否为每个数据库显示统计信息
            continue_on_error: 是否在出错时继续构建其他数据库
            
        Returns:
            构建结果统计
        """
        logger.info("*" * 100)
        logger.info(f"开始批量构建 {len(db_ids)} 个数据库")
        logger.info("*" * 100)
        
        overall_start_time = time.time()
        
        # 使用tqdm显示进度条
        with tqdm(db_ids, desc="构建数据库", unit="db", 
                  bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}") as pbar:
            
            for i, db_id in enumerate(pbar, 1):
                # 更新进度条描述
                success_count = len(self.build_stats['success'])
                failed_count = len(self.build_stats['failed'])
                pbar.set_postfix({
                    '当前': db_id,
                    '成功': success_count,
                    '失败': failed_count
                })
                
                logger.info(f"\n进度: [{i}/{len(db_ids)}] 正在处理: {db_id}")
                
                try:
                    success = self.build_single_database(
                        db_id, 
                        clear_before=clear_before_each,
                        show_stats=show_stats_each
                    )
                    
                    if not success and not continue_on_error:
                        logger.error(f"数据库 {db_id} 构建失败，终止批量构建")
                        break
                        
                except KeyboardInterrupt:
                    logger.info("用户中断，停止批量构建")
                    break
                except Exception as e:
                    logger.error(f"处理数据库 {db_id} 时发生意外错误: {e}")
                    if not continue_on_error:
                        break
        
        overall_end_time = time.time()
        overall_time = overall_end_time - overall_start_time
        
        # 生成最终统计报告
        self.print_final_report(overall_time)
        
        return {
            'total_time': overall_time,
            'success_count': len(self.build_stats['success']),
            'failed_count': len(self.build_stats['failed']),
            'success_list': [item['db_id'] for item in self.build_stats['success']],
            'failed_list': [item['db_id'] for item in self.build_stats['failed']]
        }
    
    def print_final_report(self, total_time: float, max_workers: int = 1):
        """打印最终的构建报告"""
        logger.info("\n" + "=" * 100)
        logger.info("构建完成 - 最终报告")
        logger.info("=" * 100)
        
        success_count = len(self.build_stats['success'])
        failed_count = len(self.build_stats['failed'])
        total_count = success_count + failed_count
        
        logger.info(f"总体统计:")
        logger.info(f"  总数据库数: {total_count}")
        logger.info(f"  成功构建: {success_count}")
        logger.info(f"  构建失败: {failed_count}")
        logger.info(f"  成功率: {success_count/total_count*100:.2f}%" if total_count > 0 else "  成功率: 0%")
        logger.info(f"  使用线程数: {max_workers}")
        logger.info(f"  总耗时: {total_time:.2f}s")
        logger.info(f"  平均耗时: {total_time/total_count:.2f}s/db" if total_count > 0 else "  平均耗时: 0s/db")
        if max_workers > 1:
            theoretical_sequential_time = sum(item['build_time'] for item in self.build_stats['success'] + self.build_stats['failed'])
            if theoretical_sequential_time > 0:
                speedup = theoretical_sequential_time / total_time
                efficiency = speedup / max_workers * 100
                logger.info(f"  理论顺序耗时: {theoretical_sequential_time:.2f}s")
                logger.info(f"  并行加速比: {speedup:.2f}x")
                logger.info(f"  并行效率: {efficiency:.1f}%")
        
        if self.build_stats['success']:
            logger.info(f"\n成功构建的数据库 ({success_count}个):")
            for item in self.build_stats['success']:
                thread_info = f" [线程 {item['thread_id']}]" if 'thread_id' in item else ""
                logger.info(f"  ✓ {item['db_id']} ({item['build_time']:.2f}s){thread_info}")
        
        if self.build_stats['failed']:
            logger.info(f"\n构建失败的数据库 ({failed_count}个):")
            for item in self.build_stats['failed']:
                thread_info = f" [线程 {item['thread_id']}]" if 'thread_id' in item else ""
                logger.info(f"  ✗ {item['db_id']} ({item['build_time']:.2f}s){thread_info} - {item['error']}")
    
    def close(self):
        """关闭资源"""
        if self.builder:
            self.builder.close()


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='从spider2-snow.jsonl构建所有数据库的知识图谱（支持多线程）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python main.py                          # 单线程构建所有数据库
  python main.py --threads 4              # 使用4个线程并行构建
  python main.py --clear                  # 构建前清理每个数据库的现有数据
  python main.py --stats                  # 为每个数据库显示统计信息
  python main.py --single NORTHWIND       # 只构建指定数据库
  python main.py --verify-only            # 只验证数据库目录，不构建
  python main.py --list-only              # 只列出所有数据库ID
  python main.py --threads 8 --clear      # 8线程并行构建且清理数据（注意数据竞争）
        """
    )
    
    parser.add_argument('--jsonl-file', default='spider2-snow.jsonl',
                       help='JSONL文件路径 (默认: spider2-snow.jsonl)')
    parser.add_argument('--clear', action='store_true',
                       help='在每个数据库构建前清理现有图数据')
    parser.add_argument('--stats', action='store_true',
                       help='为每个数据库显示详细统计信息')
    parser.add_argument('--single', type=str,
                       help='只构建指定的单个数据库')
    parser.add_argument('--verify-only', action='store_true',
                       help='只验证数据库目录是否存在，不进行构建')
    parser.add_argument('--list-only', action='store_true',
                       help='只列出所有数据库ID，不进行构建')
    parser.add_argument('--stop-on-error', action='store_true',
                       help='遇到错误时停止构建（默认继续）')
    parser.add_argument('--threads', type=int, default=1,
                       help='并行线程数 (默认: 1, 推荐: 2-8)')
    parser.add_argument('--sequential', action='store_true',
                       help='强制使用单线程顺序模式（与 --threads 1 等效）')
    
    args = parser.parse_args()
    
    logger.info("数据库知识图谱批量构建程序")
    logger.info("=" * 60)
    
    # 初始化管理器
    manager = DatabaseGraphManager(args.jsonl_file)
    
    try:
        # 验证数据库连接
        if not manager.builder.executor.verify_connectivity():
            logger.error("数据库连接失败，程序退出")
            return 1
        
        logger.info("数据库连接成功")
        
        # 提取数据库ID
        db_ids = manager.extract_database_ids()
        if not db_ids:
            logger.error("未找到任何数据库ID")
            return 1
        
        # 如果只是列出数据库ID
        if args.list_only:
            logger.info("程序结束")
            return 0
        
        # 验证数据库目录
        available_dbs = manager.verify_database_directories(db_ids)
        if not available_dbs:
            logger.error("没有找到可用的数据库目录")
            return 1
        
        # 如果只是验证目录
        if args.verify_only:
            logger.info("目录验证完成")
            return 0
        
        # 处理线程参数
        max_workers = 1 if args.sequential else args.threads
        if max_workers < 1:
            logger.error("线程数必须大于等于1")
            return 1
        if max_workers > 16:
            logger.warning(f"线程数 {max_workers} 可能过高，建议不超过16")
        
        # 多线程模式下的额外检查
        if max_workers > 1:
            logger.info(f"多线程模式：将使用 {max_workers} 个并发线程")
            logger.info("请确保Neo4j数据库配置支持足够的并发连接")
            if args.clear:
                logger.warning("⚠️  多线程 + --clear 组合可能导致数据竞争！")
                logger.warning("    建议：先单独运行清理，再进行多线程构建")
        
        # 构建数据库
        if args.single:
            # 构建单个数据库
            if args.single not in available_dbs:
                logger.error(f"指定的数据库不可用: {args.single}")
                return 1
            
            success = manager.build_single_database(
                args.single,
                clear_before=args.clear,
                show_stats=args.stats
            )
            return 0 if success else 1
        else:
            # 批量构建所有数据库
            if max_workers == 1:
                logger.info("使用单线程模式")
                result = manager.build_all_databases(
                    available_dbs,
                    clear_before_each=args.clear,
                    show_stats_each=args.stats,
                    continue_on_error=not args.stop_on_error
                )
            else:
                logger.info(f"使用多线程模式 ({max_workers} 个线程)")
                result = manager.build_all_databases_parallel(
                    available_dbs,
                    max_workers=max_workers,
                    clear_before_each=args.clear,
                    show_stats_each=args.stats,
                    continue_on_error=not args.stop_on_error
                )
            
            return 0 if result['failed_count'] == 0 else 1
            
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
        return 1
    except Exception as e:
        logger.error(f"程序执行过程中发生错误: {e}")
        logging.exception("详细错误信息:")
        return 1
    finally:
        manager.close()
        logger.info("程序结束，数据库连接已关闭")


if __name__ == "__main__":
    sys.exit(main())
