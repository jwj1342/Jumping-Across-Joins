"""
节点创建器模块
负责创建图数据库中的各种节点类型
"""
import sys
import os
import logging
from typing import Dict, Any

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.CypherExecutor import CypherExecutor
import db2graph.CypherTemplate as templates

# 创建带有模块名的logger
logger = logging.getLogger(__name__)


class NodeCreator:
    """节点创建器类，负责创建各种类型的图节点"""
    
    def __init__(self, executor: CypherExecutor):
        """
        初始化节点创建器
        Args:
            executor: Cypher执行器实例
        """
        self.executor = executor
    
    def create_database_node(self, db_name: str) -> bool:
        """创建数据库节点"""
        cypher = templates.create_node.format(
            label="Database",
            properties=f"name: '{db_name}', type: 'database'"
        )
        success, result = self.executor.execute_transactional_cypher(cypher)
        if success:
            logger.debug(f"NodeCreator: 创建数据库节点: {db_name}")
        else:
            logger.error(f"NodeCreator: 创建数据库节点失败: {db_name}")
        return success
    
    def create_schema_node(self, db_name: str, schema_name: str, description: str = "") -> bool:
        """创建模式节点"""
        # 使用统一的转义方法
        escaped_description = self._escape_string(description)
        
        cypher = templates.create_node.format(
            label="Schema",
            properties=f"name: '{schema_name}', database: '{db_name}', description: '{escaped_description}', type: 'schema'"
        )
        success, result = self.executor.execute_transactional_cypher(cypher)
        if success:
            logger.debug(f"NodeCreator: 创建模式节点: {schema_name}")
        else:
            logger.error(f"NodeCreator: 创建模式节点失败: {schema_name}")
        return success
    
    def create_table_node(self, db_name: str, schema_name: str, table_info: Dict[str, Any], ddl: str = "") -> bool:
        """创建表节点"""
        table_name = table_info.get('table_name', '')
        table_fullname = table_info.get('table_fullname', '')
        
        # 暂时不存储DDL以避免转义问题，专注于核心功能
        # 后续可以将DDL存储在单独的节点或关系中
        ddl_summary = f"Table with {len(table_info.get('column_names', []))} columns" if ddl else ""
        
        cypher = templates.create_node.format(
            label="Table",
            properties=f"name: '{table_name}', fullname: '{table_fullname}', database: '{db_name}', schema: '{schema_name}', ddl_summary: '{ddl_summary}', type: 'table'"
        )
        success, result = self.executor.execute_transactional_cypher(cypher)
        if success:
            logger.debug(f"NodeCreator: 创建表节点: {table_name}")
        else:
            logger.error(f"NodeCreator: 创建表节点失败: {table_name}")
        return success
    
    def create_column_node(self, db_name: str, schema_name: str, table_name: str, 
                          column_name: str, column_type: str, description: str = "", 
                          sample_data: str = "") -> bool:
        """创建列节点"""
        # 使用统一的转义方法
        escaped_description = self._escape_string(description)
        escaped_sample = self._escape_string(sample_data)
        
        cypher = templates.create_node.format(
            label="Column",
            properties=f"name: '{column_name}', type: '{column_type}', database: '{db_name}', schema: '{schema_name}', table: '{table_name}', description: '{escaped_description}', sample_data: '{escaped_sample}', node_type: 'column'"
        )
        success, result = self.executor.execute_transactional_cypher(cypher)
        if success:
            logger.debug(f"NodeCreator: 创建列节点: {column_name} ({column_type})")
        else:
            logger.error(f"NodeCreator: 创建列节点失败: {column_name}")
        return success
    
    def create_shared_field_group_node(self, group_name: str, db_name: str, schema_name: str, 
                                     field_hash: str, field_count: int) -> bool:
        """创建共享字段组节点"""
        cypher = templates.create_node.format(
            label="SharedFieldGroup",
            properties=f"name: '{group_name}', database: '{db_name}', schema: '{schema_name}', field_hash: '{field_hash}', field_count: {field_count}, type: 'shared_field_group'"
        )
        success, result = self.executor.execute_transactional_cypher(cypher)
        if success:
            logger.debug(f"NodeCreator: 创建共享字段组: {group_name} ({field_count}个字段，符合群组定义)")
        else:
            logger.error(f"NodeCreator: 创建共享字段组失败: {group_name}")
        return success
    
    def create_shared_field_node(self, field_name: str, field_type: str, db_name: str, schema_name: str, 
                         group_name: str, description: str = "", sample_data: str = "") -> bool:
        """创建共享字段节点（专门用于SharedFieldGroup，包含字段组标识）"""
        try:
            # 使用统一的转义处理
            escaped_field_name = self._escape_string(field_name)
            escaped_field_type = self._escape_string(field_type)
            escaped_db_name = self._escape_string(db_name)
            escaped_schema_name = self._escape_string(schema_name)
            escaped_group_name = self._escape_string(group_name)
            escaped_description = self._escape_string(description)
            escaped_sample = self._escape_string(sample_data)
            
            cypher = templates.create_node.format(
                label="Field",
                properties=f"name: '{escaped_field_name}', type: '{escaped_field_type}', database: '{escaped_db_name}', schema: '{escaped_schema_name}', field_group: '{escaped_group_name}', description: '{escaped_description}', sample_data: '{escaped_sample}', node_type: 'shared_field'"
            )
            success, result = self.executor.execute_transactional_cypher(cypher)
            if success:
                logger.debug(f"NodeCreator: 创建共享字段节点: {field_name} ({field_type}) -> {group_name}")
            else:
                logger.error(f"NodeCreator: 创建共享字段节点失败: {field_name} - Cypher执行失败")
            return success
        except Exception as e:
            logger.error(f"NodeCreator: 创建共享字段节点异常: {field_name} - {str(e)}")
            return False
    
    def _escape_string(self, text: str) -> str:
        """正确的Cypher字符串转义方法，解决Neo4j查询中的特殊字符问题"""
        if not text:
            return ""
        
        # 转换为字符串并限制长度
        cleaned_text = str(text)
        # 更保守的长度限制，避免Cypher查询过长
        if len(cleaned_text) > 1000:
            cleaned_text = cleaned_text[:1000] + "..."
        
        # 调试代码已移除
        
        # 第一步：移除所有控制字符和不可见字符
        import re
        # 移除所有ASCII控制字符 (0-31) 和 DEL (127)
        cleaned_text = re.sub(r'[\x00-\x1f\x7f]', ' ', cleaned_text)
        
        # 第二步：强力处理所有可能的换行符和控制字符
        # 使用正则表达式移除所有空白字符（包括各种换行符）
        cleaned_text = re.sub(r'[\r\n\t\f\v\u0085\u2028\u2029]+', ' ', cleaned_text)
        
        # 额外确保移除任何剩余的换行符
        cleaned_text = cleaned_text.replace('\n', ' ')     # 标准换行符
        cleaned_text = cleaned_text.replace('\r', ' ')     # 回车符  
        cleaned_text = cleaned_text.replace('\t', ' ')     # 制表符
        cleaned_text = cleaned_text.replace('\f', ' ')     # 换页符
        cleaned_text = cleaned_text.replace('\v', ' ')     # 垂直制表符
        cleaned_text = cleaned_text.replace('\u0085', ' ') # NEL (Next Line)
        cleaned_text = cleaned_text.replace('\u2028', ' ') # 行分隔符
        cleaned_text = cleaned_text.replace('\u2029', ' ') # 段落分隔符
        
        # 第四步：在Neo4j Cypher中正确转义字符和移除问题字符
        # 关键修复：移除分号，避免CypherExecutor错误分割语句
        cleaned_text = cleaned_text.replace(';', ',')      # 分号替换为逗号
        # 在Cypher字符串字面量中，单引号用反斜杠转义
        cleaned_text = cleaned_text.replace('\\', '\\\\')  # 反斜杠转义（必须先转义反斜杠）
        cleaned_text = cleaned_text.replace("'", "\\'")    # 单引号转义
        
        # 第五步：清理多余的空格
        cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
        
        # 第六步：最后安全检查 - 移除任何残留的问题字符
        if any(ord(c) < 32 for c in cleaned_text):
            cleaned_text = ''.join(c if ord(c) >= 32 else ' ' for c in cleaned_text)
            cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
        
        # 调试代码已移除
        
        return cleaned_text
    
    def create_field_node(self, field_name: str, field_type: str, db_name: str, schema_name: str, 
                         table_name: str, description: str = "", sample_data: str = "") -> bool:
        """创建字段节点（独有字段，包含表名以确保唯一性）"""
        # 使用统一的转义方法
        escaped_description = self._escape_string(description)
        escaped_sample = self._escape_string(sample_data)
        
        cypher = templates.create_node.format(
            label="Field",
            properties=f"name: '{field_name}', type: '{field_type}', database: '{db_name}', schema: '{schema_name}', table: '{table_name}', description: '{escaped_description}', sample_data: '{escaped_sample}', node_type: 'unique_field'"
        )
        success, result = self.executor.execute_transactional_cypher(cypher)
        if success:
            logger.debug(f"NodeCreator: 创建独有字段节点: {field_name} ({field_type}) -> {table_name}")
        else:
            logger.error(f"NodeCreator: 创建独有字段节点失败: {field_name}")
        return success 