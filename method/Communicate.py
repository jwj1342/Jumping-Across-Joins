"""
通信流程：
InfoAgent → SqlAgent：提供经过LLM过滤的有用表和字段信息
"""

from typing import List, Dict, Any, Optional, Annotated
from typing_extensions import TypedDict
from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages

# ===== 数据库摘要树结构定义 =====

class TableSummary(TypedDict):
    """单个表的摘要，包含表名和字段列表"""
    table: str
    fields: List[str]

class SchemaSummary(TypedDict):
    """单个schema的摘要，包含schema名和其下的表摘要列表"""
    schema: str
    tables: List[TableSummary]

class DatabaseSummary(TypedDict):
    """整个数据库的摘要树结构"""
    database: str
    schemas: List[SchemaSummary]

# ===== SQL执行结果 =====

class SQLExecutionResult(TypedDict):
    """SQL执行结果"""
    success: bool
    sql_query: str
    result_data: Optional[List[Dict[str, Any]]]
    error_message: Optional[str]
    execution_time: Optional[float]

# ===== 系统状态（使用LangGraph内置消息管理）=====

class SimpleState(TypedDict):
    """图系统状态 - 使用LangGraph内置消息管理"""
    user_query: str
    database_id: str
    
    # 使用LangGraph的add_messages来管理对话历史
    messages: Annotated[List[BaseMessage], add_messages]
    
    # InfoAgent相关
    schema_info: DatabaseSummary
    
    # SqlAgent相关  
    generated_sql: str
    execution_result: Dict[str, Any]
    
    # 流程控制
    step: str
    iteration: int
    retry_count: int  # 直接在状态中管理重试次数
    max_retries: int  # 最大重试次数
    
    # 结果
    final_sql: str
    final_result: List[Dict[str, Any]]
    error_message: str
    is_completed: bool


"""
Pydantic models for structured output parsing in InfoAgent and SqlAgent.
"""

from typing import List, Optional
from pydantic import BaseModel, Field

class SqlQueryResponse(BaseModel):
    """Response model for SQL query generation."""
    sql_query: str = Field(
        description="The generated SQL query"
    )
    explanation: str = Field(
        description="Explanation of what the SQL query does"
    )
    potential_issues: Optional[str] = Field(
        default=None,
        description="Any potential issues or notes about the query"
    )

# ===== SQL错误修复上下文（简化版）=====

class SqlErrorContext(TypedDict):
    """SQL错误上下文信息 - 简化版，使用LangGraph消息管理"""
    user_query: str
    original_sql: str
    error_message: str
    database_id: str
    schema_info: DatabaseSummary
    retry_count: int
    max_retries: int
    # 注意：消息历史直接由LangGraph状态中的messages字段管理

# ===== 工具函数 =====

def can_retry(state: SimpleState) -> bool:
    """检查是否可以重试"""
    return state["retry_count"] < state["max_retries"]

def increment_retry(state: SimpleState) -> Dict[str, Any]:
    """增加重试次数，返回状态更新"""
    return {"retry_count": state["retry_count"] + 1}

def create_error_context(state: SimpleState) -> SqlErrorContext:
    """从系统状态创建错误上下文"""
    return {
        "user_query": state["user_query"],
        "original_sql": state["generated_sql"],
        "error_message": state["error_message"],
        "database_id": state["database_id"],
        "schema_info": state["schema_info"],
        "retry_count": state["retry_count"],
        "max_retries": state["max_retries"]
    }