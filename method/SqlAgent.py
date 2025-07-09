"""
SqlAgent - 纯函数式实现
将SqlAgent的功能转换为简单的函数，避免类和复杂状态管理
"""

import json
import logging
import time
import sys
from pathlib import Path
from typing import Dict, Any, Optional
import re

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.SnowConnect import snowflake_sql_query
from utils.init_llm import initialize_llm
from prompts import SQL_AGENT_PROMPT, sql_parser
from method.Communicate import DatabaseSummary

# 全局资源 - 延迟初始化
_llm = None
_logger = logging.getLogger(__name__)


def _get_llm():
    """获取全局LLM实例"""
    global _llm
    if _llm is None:
        _llm = initialize_llm()
    return _llm


# ===== 核心函数式API =====

def generate_sql(user_query: str, schema_info: DatabaseSummary, database_id: str) -> str:
    """
    生成SQL语句 - 函数式版本
    
    Args:
        user_query: 用户查询
        schema_info: Schema信息 (结构化的数据库摘要树)
        database_id: 数据库ID
        
    Returns:
        生成的SQL语句
    """
    try:
        llm = _get_llm()
        if not llm:
            _logger.warning("LLM未初始化，无法生成SQL")
            return "-- LLM未初始化，无法生成SQL"
        
        # 创建chain
        chain = SQL_AGENT_PROMPT | llm | sql_parser
        
        # 调用chain
        response = chain.invoke({
            "user_query": user_query,
            "database_id": database_id,
            "schema_info": json.dumps(schema_info, indent=2, ensure_ascii=False),
            "execution_history": "无执行历史"
        })
        
        # 记录原始响应以便调试
        _logger.info(f"LLM 响应 (类型: {type(response)}): {str(response)[:200]}...")
        
        # 检查响应类型并提取SQL
        if isinstance(response, dict):
            # 如果是字典，直接提取
            sql_query = response.get('sql_query', '')
            _logger.debug(f"从字典响应中提取SQL: {sql_query}")
        elif hasattr(response, 'sql_query'):
            # 如果是Pydantic模型，直接访问属性
            sql_query = response.sql_query
            _logger.debug(f"从Pydantic模型响应中提取SQL: {sql_query}")
        else:
            # 否则，作为字符串处理
            response_text = str(response)
            try:
                # 尝试解析JSON
                data = json.loads(response_text)
                sql_query = data.get('sql_query', '')
            except json.JSONDecodeError:
                # 最终回退到从Markdown代码块提取
                _logger.warning("JSON解析失败")
                
        return sql_query
            
    except Exception as e:
        _logger.error(f"生成SQL失败: {e}")
        return f"-- SQL生成失败: {e}"


def run_sql(user_query: str, schema_info: DatabaseSummary, database_id: str) -> Dict[str, Any]:
    """
    处理完整的查询流程 - 函数式版本
    这是SqlAgent的主要入口函数
    
    Args:
        user_query: 用户查询
        schema_info: Schema信息 (结构化的数据库摘要树)
        database_id: 数据库ID
        
    Returns:
        包含SQL和执行结果的完整响应
    """
    try:
        _logger.info(f"开始处理查询: {user_query}")
        
        # 1. 生成SQL
        sql_query = generate_sql(user_query, schema_info, database_id)
        
        # 1.5. 验证生成的SQL（放宽检查）
        if not sql_query:
            _logger.warning(f"生成的SQL为空，尝试继续处理")
            sql_query = "-- 生成的SQL为空"
        
        # 2. 直接执行SQL（内联执行逻辑）
        start_time = time.time()
        
        try:
            _logger.info(f"开始执行SQL查询，数据库：{database_id}")
            _logger.debug(f"SQL查询: {sql_query[:200]}...")
            
            
            # 放宽SQL验证 - 即使是注释也尝试执行
            if not sql_query or sql_query.strip().startswith('--'):
                _logger.warning("SQL语句为空或为注释，跳过执行")
                execution_result = {
                    "success": False,
                    "sql_query": sql_query,
                    "result_data": [],
                    "error_message": "SQL语句为空或为注释",
                    "execution_time": time.time() - start_time
                }
            else:
                # 执行查询 - 捕获所有错误但不抛出异常
                try:
                    result_data = snowflake_sql_query(sql_query, database_id)
                    execution_time = time.time() - start_time
                    
                    _logger.info(f"SQL执行成功，返回 {len(result_data) if result_data else 0} 行数据")
                    
                    execution_result = {
                        "success": True,
                        "sql_query": sql_query,
                        "result_data": result_data or [],
                        "error_message": None,
                        "execution_time": execution_time
                    }
                
                except Exception as sql_error:
                    execution_time = time.time() - start_time
                    error_message = str(sql_error)
                    
                    _logger.warning(f"SQL执行失败，但系统继续运行: {error_message}")
                    
                    execution_result = {
                        "success": False,
                        "sql_query": sql_query,
                        "result_data": [],
                        "error_message": error_message,
                        "execution_time": execution_time
                    }
        
        except Exception as e:
            execution_time = time.time() - start_time
            error_msg = f"SQL执行过程错误: {str(e)}"
            _logger.error(error_msg)
            execution_result = {
                "success": False,
                "sql_query": sql_query,
                "result_data": [],
                "error_message": error_msg,
                "execution_time": execution_time
            }
        
        # 3. 返回完整结果
        result = {
            "user_query": user_query,
            "generated_sql": sql_query,
            "execution_result": execution_result,
            "success": execution_result["success"],
            "result_data": execution_result["result_data"],
            "error_message": execution_result["error_message"]
        }
        
        _logger.info(f"查询处理完成，成功: {result['success']}")
        return result
        
    except Exception as e:
        _logger.error(f"处理查询失败: {e}")
        return {
            "user_query": user_query,
            "generated_sql": "",
            "execution_result": None,
            "success": False,
            "result_data": None,
            "error_message": f"查询处理失败: {e}"
        }


