#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据库图构建主程序
从spider2-snow.jsonl文件中提取所有数据库ID并构建对应的知识图谱
"""

import sys
import os
import json
import logging
import argparse
from typing import List, Set
import time
from collections import defaultdict
from tqdm import tqdm

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
    """数据库图构建管理器"""
    
    def __init__(self, jsonl_file: str = "spider2-snow.jsonl"):
        """
        初始化管理器
        
        Args:
            jsonl_file: spider2-snow.jsonl文件路径
        """
        self.jsonl_file = jsonl_file
        self.builder = GraphBuilder()
        self.build_stats = defaultdict(list)  # 记录构建统计信息
        
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
    
    def print_final_report(self, total_time: float):
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
        logger.info(f"  总耗时: {total_time:.2f}s")
        logger.info(f"  平均耗时: {total_time/total_count:.2f}s/db" if total_count > 0 else "  平均耗时: 0s/db")
        
        if self.build_stats['success']:
            logger.info(f"\n成功构建的数据库 ({success_count}个):")
            for item in self.build_stats['success']:
                logger.info(f"  ✓ {item['db_id']} ({item['build_time']:.2f}s)")
        
        if self.build_stats['failed']:
            logger.info(f"\n构建失败的数据库 ({failed_count}个):")
            for item in self.build_stats['failed']:
                logger.info(f"  ✗ {item['db_id']} ({item['build_time']:.2f}s) - {item['error']}")
    
    def close(self):
        """关闭资源"""
        if self.builder:
            self.builder.close()


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='从spider2-snow.jsonl构建所有数据库的知识图谱',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python main.py                          # 构建所有数据库
  python main.py --clear                  # 构建前清理每个数据库的现有数据
  python main.py --stats                  # 为每个数据库显示统计信息
  python main.py --single NORTHWIND       # 只构建指定数据库
  python main.py --verify-only            # 只验证数据库目录，不构建
  python main.py --list-only              # 只列出所有数据库ID
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
            result = manager.build_all_databases(
                available_dbs,
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
