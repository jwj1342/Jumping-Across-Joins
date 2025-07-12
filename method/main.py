"""
SQL生成系统 - 函数式编程版本
整合了图系统和主程序功能
"""

import logging
import argparse
from pathlib import Path
import time
import sys
import json
from datetime import datetime
from typing import Dict, Any, List, Tuple
import jsonlines
import threading
import tempfile
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import os

# 将当前目录添加到Python路径中
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from Communicate import SystemState
from BuildAgentSystem import build_agent_system

# 导入连接池模块
try:
    from utils.SnowflakeConnectionPool import get_global_pool, close_global_pool, get_pool_stats
    HAS_CONNECTION_POOL = True
except ImportError:
    HAS_CONNECTION_POOL = False

# 全局配置
logger = logging.getLogger(__name__)

# 全局锁用于线程安全
file_lock = threading.Lock()
log_lock = threading.Lock()

def create_thread_workspace(thread_id: str, base_temp_dir: Path) -> Path:
    """
    为线程创建独立的临时工作目录
    
    Args:
        thread_id: 线程ID
        base_temp_dir: 基础临时目录
        
    Returns:
        线程工作目录路径
    """
    thread_dir = base_temp_dir / f"thread_{thread_id}"
    thread_dir.mkdir(parents=True, exist_ok=True)
    return thread_dir

def process_single_query_with_stats(
    item: Dict,
    results_dir: Path,
    base_temp_dir: Path,
    timeout_seconds: int = 300
) -> Dict[str, Any]:
    """
    处理单个查询并返回详细统计信息
    
    Args:
        item: 查询项
        results_dir: 结果输出目录
        base_temp_dir: 基础临时目录
        timeout_seconds: 超时时间(秒)
        
    Returns:
        包含详细统计信息的字典
    """
    thread_id = threading.current_thread().ident
    instance_id = item.get("instance_id", "unknown")
    
    try:
        # 创建线程独立工作目录
        thread_workspace = create_thread_workspace(str(thread_id), base_temp_dir)
        
        with log_lock:
            logger.info(f"开始处理查询: {instance_id}")
        
        # 提取查询信息
        instruction = item.get("instruction", "")
        db_id = item.get("db_id", "")
        
        if not instruction or not db_id:
            raise ValueError(f"查询信息不完整: instruction={bool(instruction)}, db_id={bool(db_id)}")
        
        start_time = time.time()
        
        try:
            # 构建Agent系统图
            graph = build_agent_system()
            
            # 初始状态
            initial_state: SystemState = {
                "user_query": instruction,
                "database_id": db_id,
                "schema_info": {},
                "generated_sql": "",
                "execution_result": {},
                "step": "start",
                "iteration": 0,
                "final_sql": "",
                "final_result": [],
                "error_message": "",
                "is_completed": False
            }
            
            # 运行图
            config = {"configurable": {"thread_id": f"sql_session_{thread_id}"}}
            result = graph.invoke(initial_state, config)
            
            elapsed_time = time.time() - start_time
            
            # 检查是否超时
            if elapsed_time > timeout_seconds:
                raise TimeoutError(f"SQL生成超时: {elapsed_time:.2f}秒")
            
            result['execution_time'] = elapsed_time
            
            # 根据新的成功定义：只有返回数据大于0条才算成功
            has_data = bool(result.get('final_result', [])) and len(result.get('final_result', [])) > 0
            is_completed = result.get('is_completed', False)
            success = is_completed and has_data  # 必须完成且有数据才算成功
            
            error_msg = result.get('error_message', '')
            iterations = result.get('iteration', 0)
            
            # 更新状态分类逻辑
            if success:
                status = 'success_with_data'
            elif is_completed and not has_data:
                status = 'completed_no_data'  # 完成但无数据，按新定义算失败
            else:
                status = 'failed'
            
            # 如果完成但无数据，设置错误信息
            if is_completed and not has_data and not error_msg:
                error_msg = "查询完成但未返回数据"
            
            # 保存SQL文件
            sql_file = save_sql_to_file(
                result=result,
                instance_id=instance_id,
                query=instruction,
                database_id=db_id,
                results_dir=results_dir
            )
            
            with log_lock:
                logger.info(f"完成处理查询: {instance_id}, 耗时: {elapsed_time:.2f}秒, 成功: {success}")
            
            return {
                'instance_id': instance_id,
                'success': success,
                'error_msg': error_msg if not success else "",
                'iterations': iterations,
                'has_data': has_data,
                'status': status,
                'elapsed_time': elapsed_time,
                'final_sql': result.get('final_sql', ''),
                'final_result': result.get('final_result', [])
            }
            
        except Exception as graph_error:
            error_msg = f"图执行失败: {graph_error}"
            with log_lock:
                logger.error(f"查询 {instance_id} {error_msg}")
            return {
                'instance_id': instance_id,
                'success': False,
                'error_msg': error_msg,
                'iterations': 0,
                'has_data': False,
                'status': 'failed',
                'elapsed_time': time.time() - start_time,
                'final_sql': '',
                'final_result': []
            }
        
    except TimeoutError as e:
        error_msg = f"超时错误: {str(e)}"
        with log_lock:
            logger.error(f"查询 {instance_id} 处理超时: {error_msg}")
        return {
            'instance_id': instance_id,
            'success': False,
            'error_msg': error_msg,
            'iterations': 0,
            'has_data': False,
            'status': 'failed',
            'elapsed_time': timeout_seconds
        }
        
    except Exception as e:
        error_msg = f"处理错误: {str(e)}"
        with log_lock:
            logger.error(f"查询 {instance_id} 处理失败: {error_msg}")
        return {
            'instance_id': instance_id,
            'success': False,
            'error_msg': error_msg,
            'iterations': 0,
            'has_data': False,
            'status': 'failed',
            'elapsed_time': 0
        }
    
    finally:
        # 清理线程工作目录
        try:
            if 'thread_workspace' in locals() and thread_workspace.exists():
                shutil.rmtree(thread_workspace)
        except Exception as e:
            with log_lock:
                logger.warning(f"清理线程工作目录失败: {e}")

# ===== Agent节点函数已移动到BuildAgentSystem.py中 =====



# ===== 工具函数 =====

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

def save_sql_to_file(
    result: Dict[str, Any], 
    instance_id: str, 
    query: str, 
    database_id: str, 
    results_dir: Path
) -> str:
    """
    将SQL查询结果保存到文件
    
    Args:
        result: 系统执行结果
        instance_id: 查询实例ID
        query: 原始查询
        database_id: 数据库ID
        results_dir: 结果保存目录
        
    Returns:
        保存的文件路径
    """
    try:
        output_file = results_dir / f"{instance_id}.sql"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"-- Instance ID: {instance_id}\n")
            f.write(f"-- Query: {query}\n")
            f.write(f"-- Database: {database_id}\n")
            f.write(f"-- Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"-- Execution time: {result.get('execution_time', 0):.2f}s\n")
            f.write(f"-- Success: {result.get('success', False)} (成功定义：返回数据>0条)\n")
            f.write(f"-- Iterations: {result.get('iterations', 0)}\n")
            
            if result.get('error_message'):
                f.write(f"-- Error: {result['error_message']}\n")
            
            f.write("\n")
            f.write(result.get('final_sql', '-- No SQL generated'))
        
        return str(output_file)
        
    except Exception as e:
        logger.error(f"保存SQL文件失败 {instance_id}: {e}")
        return ""

def load_queries(input_file: Path) -> List[Dict]:
    """
    加载查询数据
    
    Args:
        input_file: 输入文件路径
        
    Returns:
        查询列表
    """
    queries = []
    
    try:
        with jsonlines.open(input_file) as reader:
            for item in reader:
                queries.append(item)
        
        logger.info(f"成功加载 {len(queries)} 个查询")
        return queries
        
    except Exception as e:
        logger.error(f"加载查询文件失败 {input_file}: {e}")
        return []

def create_timestamped_directory(base_path: Path, prefix: str = "results") -> Path:
    """
    创建带时间戳的目录
    
    Args:
        base_path: 基础路径
        prefix: 目录前缀
        
    Returns:
        创建的目录路径
    """
    # 生成短时间戳 (格式: YYYYMMDD_HHMMSS)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_dir = base_path / f"{prefix}_{timestamp}"
    results_dir.mkdir(exist_ok=True)
    
    logger.info(f"创建结果目录: {results_dir}")
    return results_dir

def process_batch_queries(queries: List[Dict], results_dir: Path, max_workers: int = None, timeout_seconds: int = 300) -> Dict[str, Any]:
    """
    批量处理查询
    
    Args:
        queries: 查询列表
        results_dir: 结果保存目录
        max_workers: 最大并发线程数
        timeout_seconds: 单个查询超时时间
        
    Returns:
        处理结果摘要
    """
    total_queries = len(queries)
    success_count = 0  # 成功：有数据返回
    failed_count = 0   # 失败：无数据返回或执行失败
    completed_no_data_count = 0  # 完成但无数据的数量（算作失败，但单独统计）
    total_iterations = 0
    failed_items = []
    instance_results = []  # 记录每个instance的结果
    
    logger.info(f"开始并发处理 {total_queries} 个查询...")
    logger.info(f"成功定义：在规定次数内生成出可以查询到大于一条数据的结果")
    
    # 确保结果目录存在
    results_dir.mkdir(parents=True, exist_ok=True)
    
    # 创建基础临时目录
    base_temp_dir = Path(tempfile.mkdtemp(prefix="sql_gen_batch_"))
    
    try:
        # 使用ThreadPoolExecutor进行并发处理
        if max_workers is None:
            max_workers = min(32, (os.cpu_count() or 1) + 4)
        
        # 初始化连接池
        if HAS_CONNECTION_POOL:
            try:
                pool = get_global_pool(max_connections=max_workers)
                logger.info(f"连接池已初始化: max_connections={max_workers}")
            except Exception as e:
                logger.warning(f"连接池初始化失败，将使用原始连接方式: {e}")
            
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_query = {
                executor.submit(
                    process_single_query_with_stats,  # 使用新的函数
                    item,
                    results_dir,
                    base_temp_dir,
                    timeout_seconds
                ): item for item in queries
            }
            
            # 使用as_completed异步收集结果
            with tqdm(total=len(queries), desc="处理进度") as pbar:
                for future in as_completed(future_to_query):
                    item = future_to_query[future]
                    
                    try:
                        result_data = future.result()
                        instance_id = result_data['instance_id']
                        success = result_data['success']
                        error_msg = result_data['error_msg']
                        iterations = result_data['iterations']
                        has_data = result_data['has_data']
                        status = result_data['status']
                        
                        # 记录instance结果
                        instance_result = {
                            "instance_id": instance_id,
                            "status": status,
                            "iterations": iterations,
                            "has_data": has_data
                        }
                        instance_results.append(instance_result)
                        
                        if success:
                            success_count += 1
                            total_iterations += iterations
                        else:
                            failed_count += 1
                            # 如果是完成但无数据的情况，单独统计
                            if status == 'completed_no_data':
                                completed_no_data_count += 1
                            
                            failed_items.append({
                                "instance_id": instance_id,
                                "error": error_msg,
                                "item": item
                            })
                        
                        pbar.update(1)
                        pbar.set_postfix({
                            "成功": success_count,
                            "失败": failed_count,
                            "平均轮数": f"{total_iterations/(success_count) if success_count > 0 else 0:.1f}"
                        })
                        
                    except Exception as e:
                        failed_count += 1
                        instance_id = item.get("instance_id", "unknown")
                        failed_items.append({
                            "instance_id": instance_id,
                            "error": f"Future执行异常: {str(e)}",
                            "item": item
                        })
                        
                        # 记录失败的instance结果
                        instance_results.append({
                            "instance_id": instance_id,
                            "status": "failed",
                            "iterations": 0,
                            "has_data": False
                        })
                        
                        pbar.update(1)
                        pbar.set_postfix({
                            "成功": success_count,
                            "失败": failed_count,
                            "平均轮数": f"{total_iterations/(success_count) if success_count > 0 else 0:.1f}"
                        })
    
    finally:
        # 清理临时目录
        try:
            if base_temp_dir.exists():
                shutil.rmtree(base_temp_dir)
                logger.info("批量处理临时目录已清理")
        except Exception as e:
            logger.warning(f"清理批量处理临时目录失败: {e}")
        
        # 输出连接池统计信息
        if HAS_CONNECTION_POOL:
            try:
                stats = get_pool_stats()
                logger.info(f"连接池统计信息: {stats}")
            except Exception as e:
                logger.warning(f"获取连接池统计信息失败: {e}")
    
    # 保存失败记录
    if failed_items:
        failed_report_file = results_dir / "failed_queries.json"
        with open(failed_report_file, 'w', encoding='utf-8') as f:
            json.dump(failed_items, f, ensure_ascii=False, indent=2)
        logger.info(f"失败记录已保存到: {failed_report_file}")
    
    # 保存每个instance的结果
    instance_results_file = results_dir / "instance_results.json"
    with open(instance_results_file, 'w', encoding='utf-8') as f:
        json.dump(instance_results, f, ensure_ascii=False, indent=2)
    logger.info(f"Instance结果已保存到: {instance_results_file}")
    
    # 生成汇总报告
    avg_iterations = total_iterations / success_count if success_count > 0 else 0
    
    summary_report = {
        "total_queries": total_queries,
        "successful": success_count,
        "failed": failed_count,
        "success_rate": f"{success_count/total_queries*100:.2f}%" if total_queries > 0 else "0%",
        "average_iterations": f"{avg_iterations:.2f}",
        "completed_no_data_count": completed_no_data_count,
        "success_definition": "成功定义：在规定次数内生成出可以查询到大于一条数据的结果",
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "results_directory": str(results_dir)
    }
    
    summary_file = results_dir / "summary_report.json"
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary_report, f, ensure_ascii=False, indent=2)
    
    logger.info(f"汇总报告已保存到: {summary_file}")
    
    return summary_report

def main():
    """主函数 - 批量处理模式"""
    # 设置日志
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("启动SQL生成系统 - 批量处理模式")
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='SQL生成系统 - 批量处理模式')
    parser.add_argument('--input-file', '-i', type=str, default='spider2-snow-instances-nodata.jsonl', help='输入文件路径')
    parser.add_argument('--max-workers', type=int, default=min(16, (os.cpu_count() or 1) + 4), help='最大工作线程数')
    parser.add_argument('--timeout', type=int, default=300, help='单个查询超时时间(秒)')
    
    args = parser.parse_args()
    
    try:
        # 获取当前目录
        current_dir = Path(__file__).parent
        
        print("SQL生成系统 - 批量处理模式")
        print("="*50)
        
        # 检查输入文件
        input_file = current_dir / args.input_file
        if not input_file.exists():
            logger.error(f"输入文件不存在: {input_file}")
            print(f"错误: 输入文件不存在: {input_file}")
            sys.exit(1)
        
        # 加载查询数据
        queries = load_queries(input_file)
        if not queries:
            logger.error("没有找到查询数据，程序退出")
            print("错误: 没有找到查询数据")
            sys.exit(1)
        
        print(f"加载了 {len(queries)} 个查询")
        print(f"输入文件: {input_file}")
        
        # 创建结果目录
        results_dir = create_timestamped_directory(current_dir, "batch_results")
        print(f"结果将保存到: {results_dir}")
        print("\n开始批量处理...")
        
        # 使用process_batch_queries函数
        start_time = time.time()
        summary_report = process_batch_queries(queries, results_dir, args.max_workers, args.timeout)
        total_time = time.time() - start_time
        
        # 更新汇总报告添加时间信息
        summary_report.update({
            "max_workers": args.max_workers,
            "timeout_seconds": args.timeout,
            "total_time": f"{total_time:.2f}秒",
            "avg_time_per_query": f"{total_time/len(queries):.2f}秒"
        })
        
        # 重新保存更新后的汇总报告
        summary_file = results_dir / "summary_report.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary_report, f, ensure_ascii=False, indent=2)
        
        # 显示处理结果
        print("\n" + "="*60)
        print("批量处理完成！")
        print("="*60)
        print(f"📊 处理结果")
        print("-" * 60)
        print(f"总查询数: {summary_report['total_queries']}")
        print(f"成功处理: {summary_report['successful']} (有数据返回)")
        print(f"处理失败: {summary_report['failed']} (无数据返回或执行失败)")
        print(f"成功率: {summary_report['success_rate']}")
        print(f"平均修复轮数: {summary_report['average_iterations']}")
        print(f"完成但无数据: {summary_report['completed_no_data_count']}")
        print(f"总耗时: {summary_report.get('total_time', '未知')}")
        print(f"平均每个查询: {summary_report.get('avg_time_per_query', '未知')}")
        print(f"并发线程数: {args.max_workers}")
        print(f"结果目录: {results_dir}")
        print("="*60)
        
        if summary_report['failed'] > 0:
            print(f"\n⚠️  有 {summary_report['failed']} 个查询处理失败，详细信息请查看 failed_queries.json")
        
        print(f"\n📊 详细统计信息已保存到 instance_results.json")
        
        # 显示连接池统计信息
        if HAS_CONNECTION_POOL:
            try:
                stats = get_pool_stats()
                print(f"\n🔗 连接池统计信息:")
                print(f"  总创建连接数: {stats.get('total_created', 0)}")
                print(f"  总销毁连接数: {stats.get('total_destroyed', 0)}")
                print(f"  总借用连接数: {stats.get('total_borrowed', 0)}")
                print(f"  总归还连接数: {stats.get('total_returned', 0)}")
                print(f"  总重试次数: {stats.get('total_retries', 0)}")
                print(f"  当前连接池大小: {stats.get('pool_size', 0)}")
                print(f"  当前活跃连接数: {stats.get('current_active', 0)}")
            except Exception as e:
                print(f"⚠️  获取连接池统计信息失败: {e}")
        
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
        print("\n\n程序被用户中断")
        
        # 清理连接池
        if HAS_CONNECTION_POOL:
            try:
                close_global_pool()
                logger.info("连接池已关闭")
            except Exception as e:
                logger.warning(f"关闭连接池失败: {e}")
        
        sys.exit(0)
    except Exception as e:
        logger.error(f"主程序执行错误: {e}")
        print(f"发生错误: {e}")
        
        # 清理连接池
        if HAS_CONNECTION_POOL:
            try:
                close_global_pool()
                logger.info("连接池已关闭")
            except Exception as e:
                logger.warning(f"关闭连接池失败: {e}")
        
        sys.exit(1)
    
    finally:
        # 确保连接池被正确关闭
        if HAS_CONNECTION_POOL:
            try:
                close_global_pool()
                logger.info("连接池已关闭")
            except Exception as e:
                logger.warning(f"关闭连接池失败: {e}")

if __name__ == "__main__":
    main()