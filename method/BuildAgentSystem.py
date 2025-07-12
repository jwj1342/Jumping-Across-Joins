"""
Agent系统构建模块
包含Agent节点函数、路由函数和图构建逻辑
"""

import logging
from typing import Dict, Any, Union
from langgraph.graph import StateGraph, END
from langgraph.types import Send

from Communicate import SystemState

# 全局配置
logger = logging.getLogger(__name__)

# ===== InfoAgent 实现 =====

def info_agent_node(state: SystemState) -> Dict[str, Any]:
    """InfoAgent节点 - 纯函数式实现，支持智能摘要生成"""
    try:
        logger.info("InfoAgent开始处理schema信息")
        
        # 使用纯函数式InfoAgent
        from InfoAgent import get_intelligent_db_summary
        
        user_query = state.get("user_query", "")
        database_id = state["database_id"]
        
        logger.info(f"基于用户查询生成智能摘要: {user_query[:100]}...")
        # 使用智能摘要生成
        db_summary = get_intelligent_db_summary(database_id, user_query)
        
        
        if not db_summary:
            logger.error(f"无法为数据库 {database_id} 获取摘要树，流程终止。")
            return {
                **state,
                "step": "error",
                "error_message": f"InfoAgent错误: 未能获取数据库摘要树。",
                "is_completed": True
            }

        # 记录摘要类型
        summary_type = "智能摘要" if user_query and user_query.strip() else "默认摘要"
        logger.info(f"InfoAgent完成，已成功获取{summary_type}。")
        
        # 使用Send API发送到SqlAgent
        return Send("sql_agent_node", {
            **state,
            "schema_info": db_summary,
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

def sql_agent_node(state: SystemState) -> Union[Dict[str, Any], Send]:
    """SqlAgent节点 - 纯函数式实现"""
    try:
        logger.info("SqlAgent开始生成和执行SQL")
        
        # 使用纯函数式SqlAgent
        from SqlAgent import run_sql_agent
        
        # 处理完整查询流程
        result = run_sql_agent(
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
            # 执行失败，SqlAgent内部已经处理了错误分析和重试逻辑
            logger.error(f"SQL执行失败，处理完成: {result.get('error_message', '')}")
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

def result_handler_node(state: SystemState) -> Dict[str, Any]:
    """结果处理节点"""
    logger.info("处理最终结果")
    
    return {
        **state,
        "step": "completed",
        "is_completed": True
    }

# ===== 路由函数 =====

def route_completion(state: SystemState) -> str:
    """路由到完成状态"""
    if state["is_completed"]:
        return "end"
    return "continue"

# ===== 图构建函数 =====

def build_agent_system() -> StateGraph:
    """
    构建Agent系统图
    
    Returns:
        编译好的StateGraph对象
    """
    logger.info("构建Agent系统图...")
    
    # 创建图
    workflow = StateGraph(SystemState)
    
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
    
    logger.info("Agent系统图构建完成")
    return graph
