"""
SqlAgent - SQL生成和执行代理
使用LangGraph Tool重构的SQL执行功能，并集成错误分析和修复能力
"""

import logging
import json
import sys
import time
from pathlib import Path
from typing import Dict, Any, List

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.SnowConnect import snowflake_sql_query
from utils.init_llm import initialize_llm

# 尝试导入连接池功能
try:
    from utils.SnowflakeConnectionPool import snowflake_sql_query_with_pool
    HAS_CONNECTION_POOL = True
except ImportError:
    HAS_CONNECTION_POOL = False
from prompts import SQL_AGENT_PROMPT, sql_parser, ERROR_ANALYSIS_PROMPT, error_analysis_parser, SQL_FIX_PROMPT, sql_fix_parser, SQL_UNDERSTANDING_PROMPT, sql_understanding_parser
from method.Communicate import DatabaseSummary, SqlErrorContext
from langchain_core.tools import tool

# 全局资源 - 延迟初始化
_llm = None
_logger = logging.getLogger(__name__)

def _get_llm():
    """获取全局LLM实例"""
    global _llm
    if _llm is None:
        _llm = initialize_llm()
    return _llm

@tool
def sql_execution_tool(sql_query: str, database_id: str) -> Dict[str, Any]:
    """
    执行SQL查询的LangGraph Tool
    
    Args:
        sql_query: SQL查询语句
        database_id: 数据库ID
        
    Returns:
        执行结果字典
    """
    start_time = time.time()
    
    try:
        _logger.debug(f"Tool执行SQL查询: {sql_query[:200]}...")
        
        # 基本SQL验证
        if not sql_query or sql_query.strip().startswith('--'):
            return {
                "success": False,
                "sql_query": sql_query,
                "result_data": [],
                "error_message": "SQL语句为空或为注释",
                "execution_time": time.time() - start_time
            }
        
        # 执行查询 - 优先使用连接池
        if HAS_CONNECTION_POOL:
            try:
                result_data = snowflake_sql_query_with_pool(sql_query, database_id, timeout=60, log=False)
            except Exception as e:
                _logger.warning(f"连接池查询失败，回退到原始连接: {e}")
                result_data = snowflake_sql_query(sql_query, database_id, timeout=60, log=False, use_pool=False)
        else:
            result_data = snowflake_sql_query(sql_query, database_id, timeout=60, log=False)
        
        execution_time = time.time() - start_time
        
        return {
            "success": True,
            "sql_query": sql_query,
            "result_data": result_data or [],
            "error_message": None,
            "execution_time": execution_time
        }
        
    except Exception as sql_error:
        execution_time = time.time() - start_time
        error_message = str(sql_error)
        
        return {
            "success": False,
            "sql_query": sql_query,
            "result_data": [],
            "error_message": error_message,
            "execution_time": execution_time
        }

# ===== 错误分析和修复功能 =====

def generate_sql_understanding(
    user_query: str,
    sql_query: str,
    database_id: str,
    schema_info: DatabaseSummary
) -> Dict[str, Any]:
    """
    生成SQL语句的语义理解
    
    Args:
        user_query: 用户查询
        sql_query: SQL语句
        database_id: 数据库ID
        schema_info: Schema信息
        
    Returns:
        SQL语义理解结果
    """
    try:
        llm = _get_llm()
        if not llm:
            _logger.warning("LLM未初始化，无法生成SQL理解")
            return {
                "sql_understanding": "LLM未初始化，无法生成SQL语义理解",
                "expected_behavior": "无法分析预期行为"
            }
        
        # 创建SQL理解chain
        understanding_chain = SQL_UNDERSTANDING_PROMPT | llm | sql_understanding_parser
        
        # 调用理解分析
        response = understanding_chain.invoke({
            "user_query": user_query,
            "sql_query": sql_query,
            "database_id": database_id,
            "schema_info": json.dumps(schema_info, indent=2, ensure_ascii=False)
        })
        
        _logger.debug(f"SQL语义理解: {response}")
        
        # 检查响应类型并提取结果
        if isinstance(response, dict):
            return response
        elif hasattr(response, 'sql_understanding'):
            return {
                "sql_understanding": response.sql_understanding,
                "expected_behavior": response.expected_behavior
            }
        else:
            _logger.warning(f"未知的SQL理解响应格式: {type(response)}")
            return {
                "sql_understanding": "SQL理解响应格式异常",
                "expected_behavior": "无法分析预期行为"
            }
            
    except Exception as e:
        _logger.error(f"SQL语义理解失败: {e}")
        return {
            "sql_understanding": f"SQL语义理解失败: {e}",
            "expected_behavior": "分析失败"
        }


def analyze_sql_error(
    user_query: str,
    generated_sql: str,
    error_message: str,
    database_id: str,
    schema_info: DatabaseSummary,
    result_data: List = None,
    sql_understanding: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    分析SQL错误或空结果
    
    Args:
        user_query: 用户查询
        generated_sql: 生成的SQL
        error_message: 错误信息
        database_id: 数据库ID
        schema_info: Schema信息
        result_data: 执行结果数据
        sql_understanding: SQL语义理解
        
    Returns:
        错误分析结果
    """
    try:
        llm = _get_llm()
        if not llm:
            _logger.warning("LLM未初始化，无法分析错误")
            return {
                "error_type": "logic_error",
                "analysis": "LLM未初始化，无法进行错误分析",
                "suggested_action": "end_process"
            }
        
        # 创建错误分析chain
        analysis_chain = ERROR_ANALYSIS_PROMPT | llm | error_analysis_parser
        
        # 准备参数
        result_data_str = json.dumps(result_data, indent=2, ensure_ascii=False) if result_data else "[]"
        sql_understanding_str = json.dumps(sql_understanding, indent=2, ensure_ascii=False) if sql_understanding else "{}"
        
        # 调用分析
        response = analysis_chain.invoke({
            "user_query": user_query,
            "generated_sql": generated_sql,
            "error_message": error_message or "",
            "database_id": database_id,
            "schema_info": json.dumps(schema_info, indent=2, ensure_ascii=False),
            "result_data": result_data_str,
            "sql_understanding": sql_understanding_str
        })
        
        _logger.debug(f"错误分析: {response}")
        
        # 检查响应类型并提取结果
        if isinstance(response, dict):
            return response
        elif hasattr(response, 'error_type'):
            return {
                "error_type": response.error_type,
                "analysis": response.analysis,
                "suggested_action": response.suggested_action
            }
        else:
            _logger.warning(f"未知的错误分析响应格式: {type(response)}")
            return {
                "error_type": "logic_error",
                "analysis": "错误分析响应格式异常",
                "suggested_action": "end_process"
            }
            
    except Exception as e:
        _logger.error(f"错误分析失败: {e}")
        return {
            "error_type": "logic_error",
            "analysis": f"错误分析失败: {e}",
            "suggested_action": "end_process"
        }


def fix_sql_with_conversation(context: SqlErrorContext, error_analysis: Dict[str, Any], conversation_history: List[str]) -> Dict[str, Any]:
    """
    使用对话历史修复SQL - 简化版
    
    Args:
        context: SQL错误上下文
        error_analysis: 错误分析结果
        conversation_history: 对话历史列表
        
    Returns:
        修复结果
    """
    try:
        llm = _get_llm()
        if not llm:
            _logger.warning("LLM未初始化，无法修复SQL")
            return {
                "success": False,
                "fixed_sql": "",
                "explanation": "LLM未初始化，无法修复SQL"
            }
        
        # 创建修复chain
        fix_chain = SQL_FIX_PROMPT | llm | sql_fix_parser
        
        # 格式化对话历史
        formatted_history = "\n".join(conversation_history) if conversation_history else "无对话历史"
        
        # 调用修复
        response = fix_chain.invoke({
            "user_query": context["user_query"],
            "original_sql": context["original_sql"],
            "error_message": context["error_message"],
            "error_analysis": error_analysis["analysis"],
            "database_id": context["database_id"],
            "schema_info": json.dumps(context["schema_info"], indent=2, ensure_ascii=False),
            "conversation_history": formatted_history
        })
        
        _logger.info(f"SQL修复完成")
        
        # 检查响应类型并提取结果
        if isinstance(response, dict):
            result = {
                "success": True,
                "fixed_sql": response.get("fixed_sql", ""),
                "explanation": response.get("explanation", "")
            }
        elif hasattr(response, 'fixed_sql'):
            result = {
                "success": True,
                "fixed_sql": response.fixed_sql,
                "explanation": response.explanation
            }
        else:
            _logger.warning(f"未知的SQL修复响应格式: {type(response)}")
            result = {
                "success": False,
                "fixed_sql": "",
                "explanation": "SQL修复响应格式异常"
            }
        
        return result
            
    except Exception as e:
        _logger.error(f"SQL修复失败: {e}")
        return {
            "success": False,
            "fixed_sql": "",
            "explanation": f"SQL修复失败: {e}"
        }


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


def run_sql_agent(user_query: str, schema_info: DatabaseSummary, database_id: str) -> Dict[str, Any]:
    """
    处理完整的查询流程 - 包含错误分析和修复功能
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
        
        # 1. 生成初始SQL
        original_sql = generate_sql(user_query, schema_info, database_id)
        current_sql = original_sql
        
        # 验证生成的SQL
        if not current_sql:
            _logger.warning(f"生成的SQL为空")
            return {
                "user_query": user_query,
                "generated_sql": "",
                "execution_result": None,
                "success": False,
                "result_data": None,
                "error_message": "SQL生成失败：生成的SQL为空"
            }
        
        # 2. 执行SQL并处理错误重试逻辑
        execution_result = None
        retry_count = 0
        max_retries = 5
        error_context = None
        
        while retry_count <= max_retries:
            _logger.info(f"第 {retry_count + 1} 次执行SQL")
            
            # 使用Tool执行SQL
            execution_result = sql_execution_tool.invoke({
                "sql_query": current_sql,
                "database_id": database_id
            })
            
            # 如果执行成功，检查是否有数据返回
            if execution_result["success"]:
                # 检查结果是否为空
                if not execution_result["result_data"] or len(execution_result["result_data"]) == 0:
                    _logger.warning(f"SQL执行成功但返回空数据")
                    
                    # 如果已达到最大重试次数，直接返回空结果
                    if retry_count >= max_retries:
                        _logger.error("已达到最大重试次数，返回空结果")
                        break
                    
                    # 生成SQL语义理解
                    sql_understanding = generate_sql_understanding(
                        user_query=user_query,
                        sql_query=current_sql,
                        database_id=database_id,
                        schema_info=schema_info
                    )
                    
                    # 分析空结果问题
                    error_analysis = analyze_sql_error(
                        user_query=user_query,
                        generated_sql=current_sql,
                        error_message="",  # 没有错误信息，但结果为空
                        database_id=database_id,
                        schema_info=schema_info,
                        result_data=execution_result["result_data"],
                        sql_understanding=sql_understanding
                    )
                    
                    _logger.info(f"空结果分析: {error_analysis}")
                    
                    # 根据分析结果决定行动
                    if error_analysis["suggested_action"] == "end_process":
                        _logger.warning("分析建议接受空结果")
                        break
                    elif error_analysis["suggested_action"] == "request_more_schema":
                        _logger.warning("分析建议请求更多schema信息，但当前简化为结束处理")
                        break
                    elif error_analysis["suggested_action"] == "fix_sql":
                        # 创建错误上下文（针对空结果）
                        if error_context is None:
                            error_context = {
                                "user_query": user_query,
                                "original_sql": original_sql,
                                "error_message": "查询执行成功但返回空数据",
                                "database_id": database_id,
                                "schema_info": schema_info,
                                "retry_count": retry_count,
                                "max_retries": max_retries
                            }
                        else:
                            error_context["error_message"] = "查询执行成功但返回空数据"
                            error_context["retry_count"] = retry_count
                        
                        # 尝试修复SQL
                        fix_result = fix_sql_with_conversation(error_context, error_analysis, [])
                        
                        if fix_result["success"] and fix_result["fixed_sql"]:
                            current_sql = fix_result["fixed_sql"]
                            _logger.info(f"针对空结果的SQL已修复")
                            _logger.info(f"修复说明: {fix_result['explanation']}")
                        else:
                            _logger.error("空结果SQL修复失败，返回空结果")
                            break
                    
                    retry_count += 1
                    continue
                else:
                    # 有数据返回，执行成功
                    _logger.info(f"SQL执行成功，返回 {len(execution_result['result_data'])} 行数据")
                    break
            
            # 执行失败，进行错误分析
            _logger.warning(f"SQL执行失败: {execution_result['error_message']}")
            
            # 如果已达到最大重试次数，不再重试
            if retry_count >= max_retries:
                _logger.error("已达到最大重试次数，停止重试")
                break
            
            # 创建或更新错误上下文
            if error_context is None:
                error_context = {
                    "user_query": user_query,
                    "original_sql": original_sql,
                    "error_message": execution_result["error_message"],
                    "database_id": database_id,
                    "schema_info": schema_info,
                    "retry_count": retry_count,
                    "max_retries": max_retries
                }
            else:
                error_context["error_message"] = execution_result["error_message"]
                error_context["retry_count"] = retry_count
            
            # 分析错误
            error_analysis = analyze_sql_error(
                user_query=user_query,
                generated_sql=current_sql,
                error_message=execution_result["error_message"],
                database_id=database_id,
                schema_info=schema_info,
                result_data=execution_result["result_data"],
                sql_understanding=generate_sql_understanding(user_query, current_sql, database_id, schema_info)
            )
            
            _logger.info(f"错误分析结果: {error_analysis}")
            
            # 根据分析结果决定行动
            if error_analysis["suggested_action"] == "end_process":
                _logger.error("错误分析建议结束处理")
                break
            elif error_analysis["suggested_action"] == "request_more_schema":
                _logger.warning("错误分析建议请求更多schema信息，但当前简化为结束处理")
                break
            elif error_analysis["suggested_action"] == "fix_sql":
                # 尝试修复SQL
                fix_result = fix_sql_with_conversation(error_context, error_analysis, [])
                
                if fix_result["success"] and fix_result["fixed_sql"]:
                    current_sql = fix_result["fixed_sql"]
                    _logger.info(f"SQL已修复")
                    _logger.info(f"修复说明: {fix_result['explanation']}")
                else:
                    _logger.error("SQL修复失败，停止重试")
                    break
            
            retry_count += 1
        
        # 3. 返回最终结果
        # 判断是否真正成功：需要SQL执行成功且有数据返回
        has_data = execution_result and execution_result["result_data"] and len(execution_result["result_data"]) > 0
        sql_execution_success = execution_result and execution_result["success"]
        final_success = sql_execution_success and has_data
        
        result = {
            "user_query": user_query,
            "generated_sql": current_sql,
            "execution_result": execution_result,
            "success": final_success,
            "result_data": execution_result["result_data"] if execution_result else None,
            "error_message": execution_result["error_message"] if execution_result and execution_result["error_message"] else ("查询未返回数据" if sql_execution_success and not has_data else "SQL执行失败"),
            "retry_count": retry_count
        }
        
        _logger.info(f"查询处理完成，最终成功: {final_success}, SQL执行成功: {sql_execution_success}, 有数据: {has_data}, 重试次数: {retry_count}")
        return result
        
    except Exception as e:
        _logger.error(f"处理查询失败: {e}")
        return {
            "user_query": user_query,
            "generated_sql": "",
            "execution_result": None,
            "success": False,
            "result_data": None,
            "error_message": f"查询处理失败: {e}",
            "retry_count": 0
        }




