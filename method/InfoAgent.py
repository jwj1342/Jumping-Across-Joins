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
from prompts import TABLE_USEFULNESS_PROMPT, FIELD_USEFULNESS_PROMPT

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

def get_all_tables(database_id: str) -> Dict[str, Any]:
    """
    获取数据库中的所有表信息 - 函数式版本
    
    Args:
        database_id: 数据库ID
        
    Returns:
        包含所有表信息的字典
    """
    try:
        cypher_query = f"""
        MATCH (d:Database {{name: '{database_id}'}})-[:HAS_SCHEMA]->(s:Schema)-[:HAS_TABLE]->(t:Table)
        RETURN s.name as schema_name, t.name as table_name, t.type as table_type, 
               t.created as created
        """
        
        success, graph_results = _get_cypher_executor().execute_transactional_cypher(cypher_query)
        
        if not success or not graph_results:
            _logger.warning(f"未找到数据库 {database_id} 的表信息")
            return {}
        
        all_tables = {}
        for result in graph_results:
            schema_name = result['schema_name']
            table_name = result['table_name']
            full_table_name = f"{schema_name}.{table_name}"
            
            all_tables[full_table_name] = {
                'schema': schema_name,
                'table': table_name,
                'type': result.get('table_type', 'BASE TABLE'),
                'created': result.get('created'),
                'row_count': 'unknown'  # 避免使用不存在的属性
            }
        
        return all_tables
        
    except Exception as e:
        _logger.error(f"获取所有表信息失败: {e}")
        return {}


def get_table_fields(table_name: str, database_id: str) -> Dict[str, Any]:
    """
    获取指定表的字段信息 - 函数式版本
    
    Args:
        table_name: 表名（格式：schema.table）
        database_id: 数据库ID
        
    Returns:
        包含表字段信息的字典
    """
    try:
        if '.' not in table_name:
            _logger.error(f"表名格式错误，应为 schema.table: {table_name}")
            return {}
        
        schema_name, table_name_only = table_name.split('.', 1)
        
        fields_cypher = f"""
        MATCH (d:Database {{name: '{database_id}'}})-[:HAS_SCHEMA]->(s:Schema {{name: '{schema_name}'}})-[:HAS_TABLE]->(t:Table {{name: '{table_name_only}'}})
        OPTIONAL MATCH (t)-[:USES_FIELD_GROUP]->(sfg:SharedFieldGroup)-[:HAS_FIELD]->(sf:Field)
        OPTIONAL MATCH (t)-[:HAS_UNIQUE_FIELD]->(uf:Field)
        WITH t, 
             COLLECT(DISTINCT {{
                 name: sf.name, 
                 type: sf.type,
                 description: sf.description
             }}) AS shared_fields,
             COLLECT(DISTINCT {{
                 name: uf.name, 
                 type: uf.type,
                 description: uf.description
             }}) AS unique_fields
        RETURN shared_fields, unique_fields
        """
        
        success, graph_results = _get_cypher_executor().execute_transactional_cypher(fields_cypher)
        
        if not success or not graph_results:
            _logger.warning(f"从图数据库获取表 {table_name} 字段信息失败")
            return {}
        
        table_info = {
            'schema': schema_name,
            'table': table_name_only,
            'full_name': table_name,
            'fields': []
        }
        
        result = graph_results[0]
        all_fields = []
        
        # 处理共享字段
        if result.get('shared_fields'):
            for field in result['shared_fields']:
                if field['name']:
                    all_fields.append({
                        'name': field['name'],
                        'type': field.get('type', ''),
                        'description': field.get('description', ''),
                        'source': 'shared'
                    })
        
        # 处理独有字段
        if result.get('unique_fields'):
            for field in result['unique_fields']:
                if field['name']:
                    all_fields.append({
                        'name': field['name'],
                        'type': field.get('type', ''),
                        'description': field.get('description', ''),
                        'source': 'unique'
                    })
        
        all_fields.sort(key=lambda x: x['name'])
        table_info['fields'] = all_fields
        
        return table_info
        
    except Exception as e:
        _logger.error(f"获取表 {table_name} 字段信息失败: {e}")
        return {}


def filter_useful_tables(user_query: str, all_tables: Dict[str, Any]) -> List[str]:
    """
    使用LLM判断有用的表 - 函数式版本
    
    Args:
        user_query: 用户查询
        all_tables: 所有表信息
        
    Returns:
        有用的表名列表
    """
    try:
        llm = _get_llm()
        if not llm or not all_tables:
            return list(all_tables.keys())
        
        # 格式化表信息
        table_info_text = ""
        for table_name, table_data in all_tables.items():
            table_info_text += f"- {table_name} (类型: {table_data.get('type', 'TABLE')}, 行数: {table_data.get('row_count', 'unknown')})\n"
        
        # 调用LLM
        prompt = TABLE_USEFULNESS_PROMPT.format(
            user_query=user_query,
            all_tables=table_info_text
        )
        
        response = llm.invoke(prompt)
        response_text = response.content if hasattr(response, 'content') else str(response)
        
        # 记录实际LLM响应以便调试
        _logger.info(f"LLM原始响应: {response_text[:500]}...")
        
        # 尝试解析JSON响应
        useful_tables = _extract_useful_tables_from_response(response_text, list(all_tables.keys()))
        _logger.info(f"LLM判断有用的表: {useful_tables}")
        return useful_tables
            
    except Exception as e:
        _logger.error(f"过滤有用表失败: {e}")
        return list(all_tables.keys())


def filter_useful_fields(user_query: str, table_name: str, table_info: Dict[str, Any]) -> List[str]:
    """
    使用LLM判断有用的字段 - 函数式版本
    
    Args:
        user_query: 用户查询
        table_name: 表名
        table_info: 表信息
        
    Returns:
        有用的字段名列表
    """
    try:
        llm = _get_llm()
        if not llm or not table_info.get('fields'):
            return [field['name'] for field in table_info.get('fields', [])]
        
        # 格式化字段信息
        fields_info_text = ""
        for field in table_info['fields']:
            fields_info_text += f"- {field['name']} ({field.get('type', 'unknown')}) - {field.get('description', '无描述')}\n"
        
        # 调用LLM
        prompt = FIELD_USEFULNESS_PROMPT.format(
            user_query=user_query,
            table_name=table_name,
            all_fields=fields_info_text
        )
        
        response = llm.invoke(prompt)
        response_text = response.content if hasattr(response, 'content') else str(response)
        
        # 记录实际LLM响应以便调试
        _logger.info(f"LLM原始响应: {response_text[:500]}...")
        
        # 尝试解析JSON响应
        all_field_names = [field['name'] for field in table_info.get('fields', [])]
        useful_fields = _extract_useful_fields_from_response(response_text, all_field_names)
        _logger.info(f"表 {table_name} 中LLM判断有用的字段: {useful_fields}")
        return useful_fields
            
    except Exception as e:
        _logger.error(f"过滤有用字段失败: {e}")
        return [field['name'] for field in table_info.get('fields', [])]


# ===== 主要的组合函数 =====

def prepare_schema_info(user_query: str, database_id: str) -> Dict[str, Any]:
    """
    为SqlAgent准备schema信息 - 函数式版本
    这是InfoAgent的主要入口函数
    
    Args:
        user_query: 用户查询
        database_id: 数据库ID
        
    Returns:
        准备好的schema信息
    """
    try:
        _logger.info(f"开始为查询准备schema信息: {user_query}")
        
        # 1. 获取所有表
        all_tables = get_all_tables(database_id)
        if not all_tables:
            return {"error": "无法获取表信息"}
        
        _logger.info(f"获取到 {len(all_tables)} 个表")
        
        # 2. 使用LLM过滤有用的表（限制处理数量避免超时）
        if len(all_tables) > 20:
            _logger.warning(f"表数量过多 ({len(all_tables)})，跳过LLM过滤，使用前20个表")
            useful_table_names = list(all_tables.keys())[:20]
        else:
            useful_table_names = filter_useful_tables(user_query, all_tables)
        
        # 确保至少有一些表可用
        if not useful_table_names:
            _logger.warning("LLM未返回有用表，使用前5个表作为fallback")
            useful_table_names = list(all_tables.keys())[:5]
        
        # 3. 为每个有用的表获取字段信息并过滤有用字段
        schema_summary = {
            "useful_tables": {},
            "total_tables_count": len(all_tables),
            "filtered_tables_count": len(useful_table_names)
        }
        
        # 限制处理的表数量，避免过度处理
        max_tables_to_process = 10
        tables_to_process = useful_table_names[:max_tables_to_process]
        
        for i, table_name in enumerate(tables_to_process, 1):
            _logger.info(f"处理表 {i}/{len(tables_to_process)}: {table_name}")
            
            if table_name in all_tables:
                table_info = get_table_fields(table_name, database_id)
                if table_info:
                    # 如果字段太多，跳过LLM过滤以节省时间
                    if len(table_info.get("fields", [])) > 50:
                        _logger.warning(f"表 {table_name} 字段过多，跳过LLM过滤")
                        useful_fields = [field['name'] for field in table_info.get("fields", [])][:20]
                    else:
                        useful_fields = filter_useful_fields(user_query, table_name, table_info)
                    
                    schema_summary["useful_tables"][table_name] = {
                        "database": database_id,  # 添加数据库信息
                        "schema": table_info["schema"],
                        "table": table_info["table"],
                        "full_table_name": f"{database_id}.{table_name}",  # 完整表名
                        "useful_fields": useful_fields,
                        "total_fields_count": len(table_info.get("fields", [])),
                        "filtered_fields_count": len(useful_fields)
                    }
        
        _logger.info(f"Schema信息准备完成，处理了 {len(tables_to_process)} 个表")
        return schema_summary
        
    except Exception as e:
        _logger.error(f"准备schema信息失败: {e}")
        return {"error": f"准备schema信息失败: {e}"}


# ===== 辅助函数 =====

def _extract_useful_tables_from_response(response_text: str, all_table_names: List[str]) -> List[str]:
    """
    从LLM响应中提取有用的表名列表 - 健壮版本
    
    Args:
        response_text: LLM的原始响应文本
        all_table_names: 所有可用的表名列表
        
    Returns:
        有用的表名列表
    """
    try:
        # 首先尝试直接解析JSON
        result = json.loads(response_text)
        useful_tables = result.get('useful_tables', [])
        if useful_tables and isinstance(useful_tables, list):
            # 验证返回的表名是否在可用表名中
            valid_tables = [table for table in useful_tables if table in all_table_names]
            if valid_tables:
                return valid_tables
    
    except json.JSONDecodeError:
        pass
    
    # 如果JSON解析失败，尝试从响应中提取JSON块
    import re
    
    # 查找代码块中的JSON
    json_patterns = [
        r'```json\s*(.*?)\s*```',
        r'```\s*(.*?)\s*```',
        r'\{[^{}]*"useful_tables"[^{}]*\}',
    ]
    
    for pattern in json_patterns:
        matches = re.findall(pattern, response_text, re.DOTALL | re.IGNORECASE)
        for match in matches:
            try:
                result = json.loads(match.strip())
                useful_tables = result.get('useful_tables', [])
                if useful_tables and isinstance(useful_tables, list):
                    valid_tables = [table for table in useful_tables if table in all_table_names]
                    if valid_tables:
                        _logger.info(f"从代码块中成功提取表名: {valid_tables}")
                        return valid_tables
            except json.JSONDecodeError:
                continue
    
    # 如果还是失败，尝试直接从文本中找表名
    found_tables = []
    for table_name in all_table_names:
        if table_name.lower() in response_text.lower():
            found_tables.append(table_name)
    
    if found_tables:
        _logger.warning(f"JSON解析失败，从文本中匹配到表名: {found_tables[:5]}...")  # 限制数量
        return found_tables[:5]  # 限制返回数量，避免过多
    
    # 最后的fallback：返回前几个表
    _logger.warning("LLM响应解析完全失败，返回前3个表作为fallback")
    return all_table_names[:3]


def _extract_useful_fields_from_response(response_text: str, all_field_names: List[str]) -> List[str]:
    """
    从LLM响应中提取有用的字段名列表 - 健壮版本
    
    Args:
        response_text: LLM的原始响应文本
        all_field_names: 所有可用的字段名列表
        
    Returns:
        有用的字段名列表
    """
    try:
        # 首先尝试直接解析JSON
        result = json.loads(response_text)
        useful_fields = result.get('useful_fields', [])
        if useful_fields and isinstance(useful_fields, list):
            # 验证返回的字段名是否在可用字段名中
            valid_fields = [field for field in useful_fields if field in all_field_names]
            if valid_fields:
                return valid_fields
    
    except json.JSONDecodeError:
        pass
    
    # 如果JSON解析失败，尝试从响应中提取JSON块
    import re
    
    # 查找代码块中的JSON
    json_patterns = [
        r'```json\s*(.*?)\s*```',
        r'```\s*(.*?)\s*```',
        r'\{[^{}]*"useful_fields"[^{}]*\}',
    ]
    
    for pattern in json_patterns:
        matches = re.findall(pattern, response_text, re.DOTALL | re.IGNORECASE)
        for match in matches:
            try:
                result = json.loads(match.strip())
                useful_fields = result.get('useful_fields', [])
                if useful_fields and isinstance(useful_fields, list):
                    valid_fields = [field for field in useful_fields if field in all_field_names]
                    if valid_fields:
                        _logger.info(f"从代码块中成功提取字段名: {valid_fields}")
                        return valid_fields
            except json.JSONDecodeError:
                continue
    
    # 如果还是失败，尝试直接从文本中找字段名
    found_fields = []
    for field_name in all_field_names:
        if field_name.lower() in response_text.lower():
            found_fields.append(field_name)
    
    if found_fields:
        _logger.warning(f"JSON解析失败，从文本中匹配到字段名: {found_fields[:10]}...")  # 限制数量
        return found_fields[:10]  # 限制返回数量，避免过多
    
    # 最后的fallback：返回所有字段
    _logger.warning("LLM响应解析完全失败，返回所有字段作为fallback")
    return all_field_names
