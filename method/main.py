"""
在这个文件中会存在：
1. 基本的并发执行框架
2. 多agent的连接定义与输入输出定义
3. 测试脚本的执行

整个系统的初始输入为一个query语句与数据库字符串，然后通过InfoAgent与SqlAgent的合作交互完成最后的SQL生成（输出）。

"""

import os
import json
import logging
import argparse
from pathlib import Path
import time
import csv
from datetime import datetime
from typing import Dict, Any, Optional
import sys
# 将当前目录添加到Python路径中
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))
from graph_system import SQLGenerationSystem


def setup_logging() -> None:
    """设置日志配置"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('sql_generation.log'),
            logging.StreamHandler()
        ]
    )


def save_results_to_csv(results: list, filename: str) -> str:
    """
    将结果保存到CSV文件
    
    Args:
        results: 查询结果列表
        filename: 文件名
        
    Returns:
        保存的文件路径
    """
    if not results:
        return ""
    
    # 生成带时间戳的文件名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"{filename}_{timestamp}.csv"
    
    try:
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            if results:
                fieldnames = results[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(results)
        
        return csv_filename
    except Exception as e:
        logging.error(f"保存CSV文件失败: {e}")
        return ""


def print_results_summary(result: Dict[str, Any]) -> None:
    """
    打印结果摘要
    
    Args:
        result: 系统执行结果
    """
    print("\n" + "="*80)
    print("SQL生成系统执行结果")
    print("="*80)
    
    print(f"执行状态: {'成功' if result['success'] else '失败'}")
    print(f"迭代次数: {result['iterations']}")
    
    if result['final_sql']:
        print(f"\n最终SQL语句:")
        print("-" * 40)
        print(result['final_sql'])
        print("-" * 40)
    
    if result.get('final_result'):
        result_count = len(result['final_result'])
        print(f"\n查询结果: {result_count} 行数据")
        
        if result_count > 0:
            print("\n前3行数据预览:")
            print("-" * 40)
            for i, row in enumerate(result['final_result'][:3], 1):
                print(f"行 {i}: {row}")
            
            if result_count > 3:
                print(f"... 还有 {result_count - 3} 行数据")
    
    if result.get('schema_discovered'):
        schema_count = len(result['schema_discovered'])
        print(f"\n发现的Schema信息: {schema_count} 个表结构")
        
        for table_name, table_info in list(result['schema_discovered'].items())[:3]:
            if isinstance(table_info, dict) and 'fields' in table_info:
                field_count = len(table_info['fields'])
                print(f"  - {table_name}: {field_count} 个字段")
    
    if result.get('execution_history'):
        print(f"\nSQL执行历史: {len(result['execution_history'])} 次尝试")
        for i, execution in enumerate(result['execution_history'], 1):
            status = "成功" if execution.success else f"失败 ({execution.error_message})"
            print(f"  {i}. {status}")
    
    if result.get('error_info'):
        print(f"\n错误信息: {result['error_info']}")
    
    print("="*80)


def run_sql_generation_system(
    query: str,
    database_id: str,
    additional_info: str = "",
    save_to_csv: bool = True
) -> Dict[str, Any]:
    """
    运行SQL生成系统的主函数
    
    Args:
        query: 用户查询
        database_id: 数据库ID
        additional_info: 额外信息
        save_to_csv: 是否保存结果到CSV
        
    Returns:
        执行结果字典
    """
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("初始化SQL生成系统...")
        system = SQLGenerationSystem(enable_logging=True)
        
        logger.info(f"开始处理查询: {query}")
        start_time = time.time()
        
        # 运行系统
        result = system.run(
            user_query=query,
            database_id=database_id,
            additional_info=additional_info
        )
        
        execution_time = time.time() - start_time
        result['execution_time'] = execution_time
        
        logger.info(f"系统执行完成，耗时: {execution_time:.2f}秒")
        
        # 保存结果到CSV
        csv_file = ""
        if save_to_csv and result.get('final_result'):
            csv_file = save_results_to_csv(
                result['final_result'],
                f"sql_result_{database_id}"
            )
            if csv_file:
                logger.info(f"结果已保存到: {csv_file}")
                result['csv_file'] = csv_file
        
        return result
        
    except Exception as e:
        logger.error(f"系统执行失败: {e}")
        return {
            'success': False,
            'error': str(e),
            'final_sql': '',
            'final_result': [],
            'iterations': 0,
            'execution_time': 0
        }


def main():
    """主函数"""
    # 设置日志
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("启动SQL生成系统")
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='SQL生成系统')
    parser.add_argument('--query', '-q',default="Which Ethereum address has the top 3 smallest positive balance from transactions involving the token at address \"0xa92a861fc11b99b24296af880011b47f9cafb5ab\"?", type=str, help='用户查询语句')
    parser.add_argument('--database', '-d', type=str, default='CRYPTO', help='数据库ID (默认: CRYPTO)')
    parser.add_argument('--additional-info', '-a', type=str, default='', help='额外信息')
    parser.add_argument('--no-csv', action='store_true', help='不保存结果到CSV文件')
    
    args = parser.parse_args()
    
    try:
        
        print("SQL生成系统 - 自定义查询模式")
        print("="*50)
        print(f"查询: {args.query}")
        print(f"数据库: {args.database}")
        if args.additional_info:
            print(f"额外信息: {args.additional_info}")
        print("\n开始执行...")
        
        result = run_sql_generation_system(
            query=args.query,
            database_id=args.database,
            additional_info=args.additional_info
        )
        
        print_results_summary(result)
        
        # 输出最终状态
        if result['success']:
            logger.info("系统执行成功完成")
            print(f"\n✅ 系统执行成功，共迭代 {result['iterations']} 次")
        else:
            logger.error("系统执行失败")
            print(f"\n❌ 系统执行失败: {result.get('error', '未知错误')}")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
        print("\n\n程序被用户中断")
        sys.exit(0)
    except Exception as e:
        logger.error(f"主程序执行错误: {e}")
        print(f"发生错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()