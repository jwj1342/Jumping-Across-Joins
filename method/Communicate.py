"""
通信机制 - 精简版
InfoAgent专注于向SqlAgent发送schema信息，不再处理返回消息

精简后的通信流程：
InfoAgent → SqlAgent：提供经过LLM过滤的有用表和字段信息
"""

from typing import List, Dict, Any, Optional
from typing_extensions import TypedDict

# 精简的消息结构
class SchemaInfo(TypedDict):
    """InfoAgent向SqlAgent发送的Schema信息"""
    useful_tables: Dict[str, Any]  # 有用的表及其字段信息
    total_tables_count: int        # 总表数
    filtered_tables_count: int     # 过滤后的表数
    database_id: str              # 数据库ID

# SQL执行结果（保留给SqlAgent使用）
class SQLExecutionResult(TypedDict):
    """SQL执行结果"""
    success: bool
    sql_query: str
    result_data: Optional[List[Dict[str, Any]]]
    error_message: Optional[str]
    execution_time: Optional[float]

# 基础系统状态（精简版）
class SystemState(TypedDict):
    """系统的基础状态"""
    user_query: str
    database_id: str
    additional_info: Optional[str]
    
    # SQL相关
    current_sql: Optional[str]
    final_sql: Optional[str]
    final_result: Optional[List[Dict[str, Any]]]
    is_completed: bool

# 图系统状态
class SimpleState(TypedDict):
    """图系统状态"""
    user_query: str
    database_id: str
    
    # InfoAgent相关
    schema_info: Dict[str, Any]
    
    # SqlAgent相关  
    generated_sql: str
    execution_result: Dict[str, Any]
    
    # 流程控制
    step: str
    iteration: int
    max_iterations: int
    
    # 结果
    final_sql: str
    final_result: List[Dict[str, Any]]
    error_message: str
    is_completed: bool

# TODO: 删除了复杂的交互类型和通信机制
# 原来的 InteractionType, InfoRequest, InfoResponse 等复杂通信结构已被移除
# InfoAgent不再处理来自SqlAgent的返回消息和错误反馈
# 如果需要重新添加双向通信，可以参考git历史记录中的原始实现

# 工具函数
def create_schema_info(
    useful_tables: Dict[str, Any],
    total_tables_count: int,
    database_id: str
) -> SchemaInfo:
    """创建Schema信息对象"""
    return {
        "useful_tables": useful_tables,
        "total_tables_count": total_tables_count,
        "filtered_tables_count": len(useful_tables),
        "database_id": database_id
    }

def create_system_state(
    user_query: str,
    database_id: str,
    additional_info: Optional[str] = None
) -> SystemState:
    """创建基础系统状态"""
    return {
        "user_query": user_query,
        "database_id": database_id,
        "additional_info": additional_info,
        "current_sql": None,
        "final_sql": None,
        "final_result": None,
        "is_completed": False
    }