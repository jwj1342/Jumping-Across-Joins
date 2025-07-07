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
from prompts import SQL_AGENT_PROMPT

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

def generate_sql(user_query: str, schema_info: Dict[str, Any], database_id: str) -> str:
    """
    生成SQL语句 - 函数式版本
    
    Args:
        user_query: 用户查询
        schema_info: Schema信息
        database_id: 数据库ID
        
    Returns:
        生成的SQL语句
    """
    try:
        llm = _get_llm()
        if not llm:
            _logger.warning("LLM未初始化，无法生成SQL")
            return "-- LLM未初始化，无法生成SQL"
        
        # 使用LLM生成SQL
        prompt = SQL_AGENT_PROMPT.format(
            user_query=user_query,
            database_id=database_id,
            schema_info=json.dumps(schema_info, indent=2, ensure_ascii=False),
            execution_history="无执行历史"
        )
        
        response = llm.invoke(prompt)
        response_text = response.content if hasattr(response, 'content') else str(response)
        
        # 记录原始响应以便调试
        _logger.info(f"LLM原始响应: {response_text[:200]}...")
        
        # 尝试解析JSON响应
        try:
            sql_response = json.loads(response_text)
            sql_query = sql_response.get('sql_query', '')
            _logger.info(f"JSON解析成功，生成SQL: {sql_query}")
            return sql_query
            
        except json.JSONDecodeError:
            # 如果不是JSON格式，尝试提取SQL
            _logger.warning("JSON解析失败，尝试从响应中提取SQL")
            extracted_sql = extract_sql_from_response(response_text)
            _logger.info(f"从响应中提取SQL: {extracted_sql[:100]}...")
            return extracted_sql
            
    except Exception as e:
        _logger.error(f"生成SQL失败: {e}")
        return f"-- SQL生成失败: {e}"


def execute_sql(sql_query: str, database_id: str) -> Dict[str, Any]:
    """
    执行SQL查询 - 函数式版本
    
    Args:
        sql_query: SQL语句
        database_id: 数据库ID
        
    Returns:
        SQL执行结果
    """
    start_time = time.time()
    
    try:
        # 清理SQL语句
        cleaned_sql = clean_sql(sql_query)
        
        # 放宽SQL验证 - 即使是注释也尝试执行
        if not cleaned_sql:
            _logger.warning("SQL语句为空，跳过执行")
            return {
                "success": False,
                "sql_query": sql_query,
                "result_data": [],
                "error_message": "SQL语句为空",
                "execution_time": time.time() - start_time
            }
        
        # 执行查询 - 捕获所有错误但不抛出异常
        try:
            result_data = snowflake_sql_query(cleaned_sql, database_id)
            execution_time = time.time() - start_time
            
            _logger.info(f"SQL执行成功，返回 {len(result_data) if result_data else 0} 行数据")
            
            return {
                "success": True,
                "sql_query": cleaned_sql,
                "result_data": result_data or [],
                "error_message": None,
                "execution_time": execution_time
            }
        
        except Exception as sql_error:
            execution_time = time.time() - start_time
            error_message = str(sql_error)
            
            _logger.warning(f"SQL执行失败，但系统继续运行: {error_message}")
            
            return {
                "success": False,
                "sql_query": cleaned_sql,
                "result_data": [],
                "error_message": error_message,
                "execution_time": execution_time
            }
        
    except Exception as e:
        execution_time = time.time() - start_time
        error_message = str(e)
        
        _logger.warning(f"SQL处理过程出错，但系统继续运行: {error_message}")
        
        return {
            "success": False,
            "sql_query": sql_query,
            "result_data": [],
            "error_message": error_message,
            "execution_time": execution_time
        }


def process_query(user_query: str, schema_info: Dict[str, Any], database_id: str) -> Dict[str, Any]:
    """
    处理完整的查询流程 - 函数式版本
    这是SqlAgent的主要入口函数
    
    Args:
        user_query: 用户查询
        schema_info: Schema信息
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
        
        # 2. 执行SQL
        execution_result = execute_sql(sql_query, database_id)
        
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


# ===== 辅助函数 =====

def extract_sql_from_response(response_text: str) -> str:
    """
    从响应文本中提取SQL语句 - 健壮版本
    
    Args:
        response_text: LLM响应文本
        
    Returns:
        提取的SQL语句
    """
    if not response_text:
        return "-- 响应为空"
    
    # 首先尝试解析JSON（可能之前的JSON解析失败了）
    try:
        # 处理可能的JSON块
        json_patterns = [
            r'```json\s*(.*?)\s*```',
            r'```\s*(.*?)\s*```',
            r'\{[^{}]*"sql_query"[^{}]*\}',
        ]
        
        for pattern in json_patterns:
            matches = re.findall(pattern, response_text, re.DOTALL | re.IGNORECASE)
            for match in matches:
                try:
                    result = json.loads(match.strip())
                    sql_query = result.get('sql_query', '')
                    if sql_query and sql_query.strip():
                        _logger.info(f"从JSON块中提取SQL: {sql_query[:50]}...")
                        return sql_query.strip()
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        _logger.debug(f"JSON提取失败: {e}")
    
    # 寻找代码块中的SQL
    code_block_pattern = r'```(?:sql)?\s*(.*?)\s*```'
    code_matches = re.findall(code_block_pattern, response_text, re.DOTALL | re.IGNORECASE)
    
    if code_matches:
        sql = code_matches[0].strip()
        if sql and not sql.startswith('{'):  # 避免返回JSON
            return sql
    
    # 寻找以SELECT开头的行
    lines = response_text.split('\n')
    for line in lines:
        line = line.strip()
        if line.upper().startswith('SELECT') and not line.startswith('{'):
            # 尝试找到完整的SQL语句
            sql_lines = [line]
            remaining_lines = lines[lines.index(line.strip()) + 1:]
            for remaining_line in remaining_lines:
                sql_lines.append(remaining_line.strip())
                if remaining_line.strip().endswith(';'):
                    break
            return '\n'.join(sql_lines)
    
    # 最后的fallback - 返回错误消息而不是原始响应
    _logger.error(f"无法从响应中提取有效SQL: {response_text[:100]}...")
    return "-- 无法提取有效的SQL语句"


def clean_sql(sql: str) -> str:
    """
    清理SQL语句 - 函数式版本
    
    Args:
        sql: 原始SQL语句
        
    Returns:
        清理后的SQL语句
    """
    if not sql:
        return ""
    
    # 移除注释
    sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
    
    # 移除多余的空白字符
    sql = re.sub(r'\s+', ' ', sql)
    sql = sql.strip()
    
    # 确保以分号结尾
    if sql and not sql.endswith(';'):
        sql += ';'
    
    return sql


def validate_sql_basic(sql: str) -> bool:
    """
    基础SQL验证 - 函数式版本
    
    Args:
        sql: SQL语句
        
    Returns:
        是否通过基础验证
    """
    if not sql or not sql.strip():
        return False
        
    sql_lower = sql.lower().strip()
    
    # 检查是否以SQL关键字开头
    valid_starts = ['select', 'with', 'show', 'describe', 'explain']
    if not any(sql_lower.startswith(start) for start in valid_starts):
        return False
    
    # 检查基本的SQL结构
    if sql_lower.startswith('select'):
        return 'from' in sql_lower or 'dual' in sql_lower
    
    return True