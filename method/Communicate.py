"""
这个文件中会定义两个agent的通讯相关的base model 以及相关的state的构造和某些的输入输出state的构造
SQLAgent → InfoAgent：信息查询请求类型：
| 交互类型            | 描述                              | 示例内容                                |
| --------------- | ------------------------------- | ----------------------------------- |
| 1. 初始schema询问   | 用户query到来后，询问数据库的大致结构           | "有哪些表？它们分别是什么？"                     |
| 2. 表字段询问        | 在尝试使用某表或字段时报错后，请求进一步的字段信息       | "表 `users` 有哪些字段？"                  |
| 3. 字段含义/共享字段组   | 想知道某字段是否与其他表共享                  | "字段 `user_id` 是不是共享字段？在哪些表中出现？"     |
| 4. 表间关系探索       | 推测Join关系时需要知道哪些表存在共享字段          | "与 `orders` 表可自然Join的表有哪些？"         |
| 5. 错误信息反馈（主动回传） | 在SQL失败时，把错误信息传回InfoAgent以触发结构探索 | "表 `customer` 不存在，可能是 `customers`？" |

InfoAgent → SQLAgent：结构信息回应类型：
| 交互类型      | 描述                       | 示例内容                                                   |
| --------- | ------------------------ | ------------------------------------------------------ |
| 1. 表结构汇总  | 提供某个表的字段及其类型             | "表 `users` 有字段：`user_id`(INT), `name`(TEXT)"           |
| 2. 字段组信息  | 共享字段出现在哪些表中              | "字段 `product_id` 是共享字段，出现于表 `orders`, `inventory`"     |
| 3. 纠错建议   | 基于报错猜测正确的表/字段名           | "表 `customer` 不存在，是否为 `customers`？"                    |
| 4. Join建议 | 指出表之间的潜在连接关系             | "`orders` 可通过字段 `user_id` Join 到 `users` 表"            |
| 5. 全局摘要   | 若SQLAgent请求全局结构概览，生成文本摘要 | "数据库包括 3 张表，分别为...。`orders` 与 `users` 通过 `user_id` 相连" |


系统的整体输入为：
1. 用户query语句
2. 数据库字符串
3. 额外的信息（可选的md文件）

系统的整体输出为：
1. 最终的SQL语句
2. 最终的SQL语句的执行结果csv文件（如果执行成功）

"""

from typing import Annotated, List, Dict, Any, Optional, Union
from typing_extensions import TypedDict
from enum import Enum
import json

# 定义reducer函数
def replace_reducer(x: Any, y: Any) -> Any:
    """默认的替换reducer"""
    return y

def list_append_reducer(existing: List[Any], new: Any) -> List[Any]:
    """列表追加reducer"""
    if existing is None:
        existing = []
    if isinstance(new, list):
        return existing + new
    return existing + [new]

def dict_merge_reducer(existing: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
    """字典合并reducer"""
    if existing is None:
        return new
    if new is None:
        return existing
    return {**existing, **new}

def increment_reducer(existing: int, new: int) -> int:
    """递增reducer"""
    return existing + new

# 定义交互类型枚举
class InteractionType(str, Enum):
    INITIAL_SCHEMA = "initial_schema"
    TABLE_FIELDS = "table_fields"
    FIELD_MEANING = "field_meaning"
    TABLE_RELATIONSHIPS = "table_relationships"
    ERROR_FEEDBACK = "error_feedback"
    SCHEMA_SUMMARY = "schema_summary"
    CORRECTION_SUGGESTION = "correction_suggestion"
    JOIN_SUGGESTION = "join_suggestion"
    GLOBAL_SUMMARY = "global_summary"

# 定义消息类型
class AgentMessage(TypedDict):
    """Agent间通信的基础消息结构"""
    message_type: InteractionType
    content: str
    metadata: Dict[str, Any]
    timestamp: Optional[str]

# SQLAgent → InfoAgent 的请求消息
class InfoRequest(TypedDict):
    """SQLAgent向InfoAgent的信息请求"""
    message_type: InteractionType
    content: str
    metadata: Dict[str, Any]
    timestamp: Optional[str]
    query_context: str
    error_info: Optional[str]
    specific_tables: List[str]
    specific_fields: List[str]

# InfoAgent → SQLAgent 的响应消息
class InfoResponse(TypedDict):
    """InfoAgent向SQLAgent的信息响应"""
    message_type: InteractionType
    content: str
    metadata: Dict[str, Any]
    timestamp: Optional[str]
    tables_info: Dict[str, Any]
    relationships: List[str]
    suggestions: List[str]

# SQL执行结果
class SQLExecutionResult(TypedDict):
    """SQL执行结果"""
    success: bool
    sql_query: str
    result_data: Optional[List[Dict[str, Any]]]
    error_message: Optional[str]
    execution_time: Optional[float]

# 系统状态定义
class SystemState(TypedDict):
    """系统的全局状态，使用Annotated类型定义reducer"""
    # 基础信息
    user_query: Annotated[str, replace_reducer]
    database_id: Annotated[str, replace_reducer]
    additional_info: Annotated[Optional[str], replace_reducer]
    
    # 对话历史
    conversation_history: Annotated[List[AgentMessage], list_append_reducer]
    
    # 当前状态
    current_agent: Annotated[str, replace_reducer]  # "info_agent" or "sql_agent"
    iteration_count: Annotated[int, replace_reducer]
    max_iterations: Annotated[int, replace_reducer]
    
    # Schema信息
    known_schema: Annotated[Dict[str, Any], dict_merge_reducer]
    
    # SQL相关
    current_sql: Annotated[Optional[str], replace_reducer]
    sql_execution_results: Annotated[List[SQLExecutionResult], list_append_reducer]
    
    # 错误处理
    last_error: Annotated[Optional[str], replace_reducer]
    error_count: Annotated[int, replace_reducer]
    
    # 最终结果
    final_sql: Annotated[Optional[str], replace_reducer]
    final_result: Annotated[Optional[List[Dict[str, Any]]], replace_reducer]
    is_completed: Annotated[bool, replace_reducer]

# 定义节点输入输出
class InfoAgentInput(TypedDict):
    """InfoAgent节点的输入"""
    state: SystemState
    request: InfoRequest

class InfoAgentOutput(TypedDict):
    """InfoAgent节点的输出"""
    response: InfoResponse
    updated_schema: Dict[str, Any]
    next_action: str  # "continue", "complete", "error"

class SQLAgentInput(TypedDict):
    """SQLAgent节点的输入"""
    state: SystemState
    schema_info: Dict[str, Any]

class SQLAgentOutput(TypedDict):
    """SQLAgent节点的输出"""
    sql_query: str
    execution_result: Optional[SQLExecutionResult]
    next_action: str  # "execute", "request_info", "complete", "error"

# LangGraph系统状态定义
class GraphSystemState(TypedDict):
    """LangGraph系统状态，使用Annotated类型定义reducer"""
    # 基础信息
    user_query: Annotated[str, replace_reducer]
    database_id: Annotated[str, replace_reducer]
    additional_info: Annotated[str, replace_reducer]
    
    # 当前状态
    current_step: Annotated[str, replace_reducer]
    iteration_count: Annotated[int, replace_reducer]
    max_iterations: Annotated[int, replace_reducer]
    
    # Schema信息
    known_schema: Annotated[Dict[str, Any], dict_merge_reducer]
    
    # SQL相关
    current_sql: Annotated[str, replace_reducer]
    sql_execution_history: Annotated[List[Any], list_append_reducer]
    
    # 错误处理
    last_error: Annotated[str, replace_reducer]
    error_count: Annotated[int, replace_reducer]
    
    # 最终结果
    final_sql: Annotated[str, replace_reducer]
    final_result: Annotated[List[Any], replace_reducer]
    is_completed: Annotated[bool, replace_reducer]

# 工具函数已移除，现在直接使用字典创建对象

# 状态更新函数
def update_system_state(
    state: SystemState,
    **kwargs
) -> Dict[str, Any]:
    """更新系统状态（使用reducer function）"""
    # 不再使用.copy()，而是直接返回更新字典
    return {key: value for key, value in kwargs.items() if key in state}

def add_conversation_message(
    state: SystemState,
    message: AgentMessage
) -> Dict[str, Any]:
    """添加对话消息到历史记录（使用reducer function）"""
    # 使用list_append_reducer会自动处理列表追加
    return {
        'conversation_history': message
    }

def should_continue_iteration(state: SystemState) -> bool:
    """判断是否应该继续迭代"""
    return (
        not state['is_completed'] and 
        state['iteration_count'] < state['max_iterations'] and
        state['error_count'] < 3
    )

def get_current_schema_summary(state: SystemState) -> str:
    """获取当前Schema信息的摘要"""
    if not state['known_schema']:
        return "暂无Schema信息"
    
    summary = []
    for table_name, table_info in state['known_schema'].items():
        if isinstance(table_info, dict):
            fields = table_info.get('fields', [])
            summary.append(f"表 {table_name}: {len(fields)} 个字段")
    
    return "; ".join(summary) if summary else "Schema信息不完整"