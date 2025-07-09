"""
InfoAgent - 纯函数式实现
将InfoAgent的功能转换为简单的函数，避免类和复杂状态管理
"""

import json
import logging
import sys
from pathlib import Path
from typing import Dict, Any, List

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.CypherExecutor import CypherExecutor
from utils.init_llm import initialize_llm

# 全局资源 - 延迟初始化
_cypher_executor = None
_llm = None
_logger = logging.getLogger(__name__)


def _get_cypher_executor():
    """获取全局CypherExecutor实例"""
    global _cypher_executor
    if _cypher_executor is None:
        _cypher_executor = CypherExecutor(enable_info_logging=False)
    return _cypher_executor


def _get_llm():
    """获取全局LLM实例"""
    global _llm
    if _llm is None:
        _llm = initialize_llm()
    return _llm


# ===== 核心函数式API =====


def get_db_summary(database_id: str) -> Dict[str, Any]:
    """
    使用单个Cypher查询获取数据库的摘要树。
    此函数执行一个复杂的查询，该查询从图数据库中构建一个结构化的JSON对象，
    摘要了数据库、其模式、表（每个模式最多5个）以及它们的字段。

    Args:
        database_id: 目标数据库的ID。

    Returns:
        一个包含数据库摘要的字典，如果找不到则为空字典。
    """
    _logger.info(f"正在为数据库 '{database_id}' 生成摘要树...")
    
    # 该Cypher查询由用户提供，用于高效地提取数据库的结构化摘要。
    cypher_query = f"""
    MATCH (db:Database {{name: "{database_id}"}})-[:HAS_SCHEMA]->(schema:Schema)-[:HAS_TABLE]->(table:Table)
    WITH db, schema, table
    ORDER BY table.name
    WITH db, schema, COLLECT(table)[..5] AS tables
    UNWIND tables AS table
    OPTIONAL MATCH (table)-[:HAS_UNIQUE_FIELD]->(uf:Field)
    OPTIONAL MATCH (table)-[:USES_FIELD_GROUP]->(group:SharedFieldGroup)-[:HAS_FIELD]->(gf:Field)
    WITH db.name AS dbName, 
         schema.name AS schemaName, 
         table.name AS tableName, 
         COLLECT(DISTINCT uf.name) + COLLECT(DISTINCT gf.name) AS allFields
    WITH dbName, schemaName, tableName, [x IN allFields WHERE x IS NOT NULL] AS fields
    WITH dbName, schemaName, COLLECT({{
      table: tableName,
      fields: fields
    }}) AS tableSummaries
    WITH dbName, COLLECT({{
      schema: schemaName,
      tables: tableSummaries
    }}) AS schemaSummaries
    RETURN {{
      database: dbName,
      schemas: schemaSummaries
    }} AS dbSummary
    """
    
    try:
        success, graph_results = _get_cypher_executor().execute_transactional_cypher(cypher_query)
        
        if success and graph_results:
            summary = graph_results[0].get('dbSummary', {})
            _logger.info(f"成功为数据库 '{database_id}' 生成摘要树")
            return summary
        else:
            _logger.warning(f"无法为数据库 '{database_id}' 生成摘要树。")
            return {}
    except Exception as e:
        _logger.error(f"为 '{database_id}' 生成摘要树时出错: {e}")
        return {}

