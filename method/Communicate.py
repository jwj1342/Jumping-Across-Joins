"""
通信流程：
InfoAgent → SqlAgent：提供经过LLM过滤的有用表和字段信息
"""

from typing import List, Dict, Any, Optional
from typing_extensions import TypedDict

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

# ===== 现有结构更新 =====

# 精简的消息结构 (SchemaInfo 已被 DatabaseSummary 替代)

# SQL执行结果（SqlAgent使用）
class SQLExecutionResult(TypedDict):
    """SQL执行结果"""
    success: bool
    sql_query: str
    result_data: Optional[List[Dict[str, Any]]]
    error_message: Optional[str]
    execution_time: Optional[float]

# 系统状态
class SimpleState(TypedDict):
    """图系统状态"""
    user_query: str
    database_id: str
    
    # InfoAgent相关 (已更新为新的摘要树结构)
    schema_info: DatabaseSummary
    
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