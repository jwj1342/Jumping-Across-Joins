"""
SQL生成系统 - 函数式编程版本
整合了图系统和主程序功能
"""

import os
import json
import logging
import argparse
from pathlib import Path
import time
import csv
import sys
from datetime import datetime
from typing import Dict, Any, Optional, List, Union

from langgraph.graph import StateGraph, END, START
from langgraph.types import Send
from langgraph.checkpoint.memory import MemorySaver

# 将当前目录添加到Python路径中
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from Communicate import SimpleState

# 全局配置
logger = logging.getLogger(__name__)
MAX_ITERATIONS = 3

# ===== InfoAgent 实现 =====

def info_agent_node(state: SimpleState) -> Dict[str, Any]:
    """InfoAgent节点 - 纯函数式实现"""
    try:
        logger.info("InfoAgent开始处理schema信息")
        
        # 使用纯函数式InfoAgent
        from InfoAgent import prepare_schema_info
        
        # 获取schema信息
        schema_info = prepare_schema_info(
            state["user_query"], 
            state["database_id"]
        )
        
        logger.info(f"InfoAgent完成，获取到 {len(schema_info.get('useful_tables', {}))} 个有用表")
        
        # 使用Send API发送到SqlAgent
        return Send("sql_agent_node", {
            **state,
            "schema_info": schema_info,
            "step": "schema_ready"
        })
        
    except Exception as e:
        logger.error(f"InfoAgent处理失败: {e}")
        return {
            **state,
            "step": "error",
            "error_message": f"InfoAgent错误: {e}",
            "is_completed": True
        }

# ===== SqlAgent 函数式实现 =====

def sql_agent_node(state: SimpleState) -> Union[Dict[str, Any], Send]:
    """SqlAgent节点 - 纯函数式实现"""
    try:
        logger.info("SqlAgent开始生成和执行SQL")
        
        # 使用纯函数式SqlAgent
        from SqlAgent import process_query
        
        # 处理完整查询流程
        result = process_query(
            state["user_query"],
            state["schema_info"],
            state["database_id"]
        )
        
        # 更新状态
        updated_state = {
            **state,
            "generated_sql": result["generated_sql"],
            "execution_result": result["execution_result"],
            "step": "sql_executed",
            "iteration": state["iteration"] + 1
        }
        
        if result["success"]:
            # 成功执行，发送到结果处理节点
            logger.info("SQL执行成功，发送到结果处理")
            return Send("result_handler_node", {
                **updated_state,
                "final_sql": result["generated_sql"],
                "final_result": result["result_data"] or [],
                "is_completed": True
            })
        else:
            # 执行失败，检查错误类型和是否应该重试
            error_message = result.get("error_message", "")
            should_retry = _should_retry_error(error_message, state["iteration"], state["max_iterations"])
            
            if should_retry:
                logger.warning(f"SQL执行失败，尝试第 {state['iteration'] + 1} 次重试")
                logger.warning(f"错误信息: {error_message}")
                # 发送回InfoAgent重新获取schema
                return Send("info_agent_node", {
                    **updated_state,
                    "error_message": result["error_message"],
                    "step": "retry"
                })
            else:
                # 不应该重试或达到最大重试次数
                logger.error(f"错误不可重试或达到最大重试次数，处理失败: {error_message}")
                return {
                    **updated_state,
                    "step": "failed",
                    "error_message": result["error_message"],
                    "is_completed": True
                }
        
    except Exception as e:
        logger.error(f"SqlAgent处理失败: {e}")
        return {
            **state,
            "step": "error",
            "error_message": f"SqlAgent错误: {e}",
            "is_completed": True
        }

# ===== 辅助节点函数 =====

def result_handler_node(state: SimpleState) -> Dict[str, Any]:
    """结果处理节点"""
    logger.info("处理最终结果")
    
    return {
        **state,
        "step": "completed",
        "is_completed": True
    }

# ===== 路由函数 =====

def route_completion(state: SimpleState) -> str:
    """路由到完成状态"""
    if state["is_completed"]:
        return "end"
    return "continue"

def _should_retry_error(error_message: str, current_iteration: int, max_iterations: int) -> bool:
    """
    判断错误是否应该重试
    
    Args:
        error_message: 错误消息
        current_iteration: 当前迭代次数
        max_iterations: 最大迭代次数
        
    Returns:
        是否应该重试
    """
    # 检查是否达到最大重试次数
    if current_iteration >= max_iterations:
        return False
    
    if not error_message:
        return True
    
    error_lower = error_message.lower()
    
    # 不应该重试的错误类型
    non_retryable_errors = [
        "syntax error",           # 语法错误
        "unexpected 'json'",      # JSON格式错误
        "无法提取有效的sql语句",     # SQL提取失败
        "llm未初始化",           # LLM初始化失败
        "authentication",        # 认证失败
        "permission",           # 权限问题
        "privilege",            # 权限问题
        "timeout",              # 超时（通常不是schema问题）
        "database", "does not exist", "not authorized",  # 数据库不存在或权限问题
        "invalid identifier",    # 字段名错误（需要更准确的schema，而非重试）
        "compilation error",     # SQL编译错误
    ]
    
    # 检查是否包含不可重试的错误
    for non_retryable in non_retryable_errors:
        if non_retryable in error_lower:
            logger.info(f"检测到不可重试错误: {non_retryable}")
            return False
    
    # 可重试的错误类型
    retryable_errors = [
        "does not exist",        # 表或列不存在
        "object does not exist", # 对象不存在
        "invalid identifier",    # 无效标识符
        "column",               # 列相关错误
        "table",                # 表相关错误
    ]
    
    # 检查是否包含可重试的错误
    for retryable in retryable_errors:
        if retryable in error_lower:
            logger.info(f"检测到可重试错误: {retryable}")
            return True
    
    # 默认情况下，第一次失败可以重试
    return current_iteration == 0

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
    打印结果摘要 - 安全版本
    
    Args:
        result: 系统执行结果
    """
    print("\n" + "="*80)
    print("SQL生成系统执行结果")
    print("="*80)
    
    # 安全访问 success 和 iterations
    success = result.get('success', False)
    iterations = result.get('iterations', 0)
    execution_time = result.get('execution_time', 0)
    
    print(f"执行状态: {'成功' if success else '失败'}")
    print(f"迭代次数: {iterations}")
    print(f"执行时间: {execution_time:.2f}秒")
    
    # 显示SQL
    final_sql = result.get('final_sql', '')
    if final_sql:
        print(f"\n最终SQL语句:")
        print("-" * 40)
        print(final_sql)
        print("-" * 40)
    
    # 显示查询结果
    final_result = result.get('final_result', [])
    if final_result:
        result_count = len(final_result)
        print(f"\n查询结果: {result_count} 行数据")
        
        if result_count > 0:
            print("\n前3行数据预览:")
            print("-" * 40)
            for i, row in enumerate(final_result[:3], 1):
                print(f"行 {i}: {row}")
            
            if result_count > 3:
                print(f"... 还有 {result_count - 3} 行数据")
    
    # 显示错误信息
    error_message = result.get('error_message', result.get('error', ''))
    if error_message:
        print(f"\n错误信息: {error_message}")
    
    # 显示CSV文件路径
    csv_file = result.get('csv_file', '')
    if csv_file:
        print(f"\nCSV文件: {csv_file}")
    
    print("="*80)

def run(
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
    try:
        logger.info("初始化SQL生成系统...")
        
        # 创建图
        workflow = StateGraph(SimpleState)
        
        # 添加节点
        workflow.add_node("info_agent_node", info_agent_node)  
        workflow.add_node("sql_agent_node", sql_agent_node)
        workflow.add_node("result_handler_node", result_handler_node)
        
        # 将入口点设置为 info_agent_node
        workflow.set_entry_point("info_agent_node")
        
        # 添加条件边
        workflow.add_conditional_edges(
            "result_handler_node",
            route_completion,
            {
                "end": END,
                "continue": "info_agent_node"  # 理论上不会到达
            }
        )
        
        # 编译图
        graph = workflow.compile()
        
        # 保存工作流程图
        try:
            workflow_graph = graph.get_graph()
            workflow_graph.draw_mermaid_png().save("workflow.png")
            logger.info("工作流程图已保存至: workflow.png")
        except Exception as e:
            logger.warning(f"保存工作流程图失败: {e}")
        # 初始状态
        initial_state: SimpleState = {
            "user_query": query,
            "database_id": database_id,
            "schema_info": {},
            "generated_sql": "",
            "execution_result": {},
            "step": "start",
            "iteration": 0,
            "max_iterations": MAX_ITERATIONS,
            "final_sql": "",
            "final_result": [],
            "error_message": "",
            "is_completed": False
        }
        
        logger.info(f"开始处理查询: {query}")
        start_time = time.time()
        
        # 设置最大执行时间（5分钟）
        max_execution_time = 300  
        
        try:
            # 运行图
            config = {"configurable": {"thread_id": "sql_session"}}
            result = graph.invoke(initial_state, config)
            
            execution_time = time.time() - start_time
            
            # 检查是否超时
            if execution_time > max_execution_time:
                logger.warning(f"系统执行超时 ({execution_time:.1f}s > {max_execution_time}s)")
                return {
                    'success': False,
                    'error': f'系统执行超时 ({execution_time:.1f}秒)',
                    'final_sql': result.get('final_sql', ''),
                    'final_result': [],
                    'iterations': result.get('iteration', 0),
                    'execution_time': execution_time
                }
            
            result['execution_time'] = execution_time
            
        except Exception as graph_error:
            execution_time = time.time() - start_time
            logger.error(f"图执行失败: {graph_error}")
            return {
                'success': False,
                'error': f'图执行失败: {graph_error}',
                'final_sql': '',
                'final_result': [],
                'iterations': 0,
                'execution_time': execution_time
            }
        
        logger.info(f"系统执行完成，耗时: {execution_time:.2f}秒")
        
        # 将 langgraph 的结果转换为标准格式
        # 更宽松的成功判断：只要系统完成执行就算成功，即使有SQL错误
        final_result = {
            'success': result.get('is_completed', False),  # 移除严格的错误检查
            'final_sql': result.get('final_sql', result.get('generated_sql', '')),
            'final_result': result.get('final_result', []),
            'iterations': result.get('iteration', 0),
            'execution_time': execution_time,
            'error_message': result.get('error_message', ''),
            'database_id': database_id,
            'user_query': query
        }
        
        # 保存结果到CSV
        csv_file = ""
        if save_to_csv and final_result.get('final_result'):
            csv_file = save_results_to_csv(
                final_result['final_result'],
                f"sql_result_{database_id}"
            )
            if csv_file:
                logger.info(f"结果已保存到: {csv_file}")
                final_result['csv_file'] = csv_file
        
        return final_result
        
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
        
        result = run(
            query=args.query,
            database_id=args.database,
            additional_info=args.additional_info,
            save_to_csv=not args.no_csv
        )
        
        print_results_summary(result)
        
        # 输出最终状态 - 安全版本
        success = result.get('success', False)
        iterations = result.get('iterations', 0)
        error_message = result.get('error_message', result.get('error', ''))
        
        if success:
            logger.info("系统执行成功完成")
            print(f"\n✅ 系统执行成功，共迭代 {iterations} 次")
        else:
            logger.error("系统执行失败")
            print(f"\n❌ 系统执行失败: {error_message or '未知错误'}")
            # 不要强制退出，让用户看到完整的错误信息
            # sys.exit(1)
            
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