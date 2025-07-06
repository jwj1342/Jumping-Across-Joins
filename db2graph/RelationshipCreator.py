"""
关系创建器模块
负责创建图数据库中各种节点之间的关系
"""
import sys
import os
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.CypherExecutor import CypherExecutor
import db2graph.CypherTemplate as templates

# 创建带有模块名的logger
logger = logging.getLogger(__name__)


class RelationshipCreator:
    """关系创建器类，负责创建各种类型的节点关系"""
    
    def __init__(self, executor: CypherExecutor):
        """
        初始化关系创建器
        Args:
            executor: Cypher执行器实例
        """
        self.executor = executor
    
    def create_has_schema_relationship(self, db_name: str, schema_name: str) -> bool:
        """创建数据库拥有模式的关系"""
        cypher = templates.create_relationship.format(
            label1="Database",
            match1=f"name: '{db_name}'",
            label2="Schema", 
            match2=f"name: '{schema_name}', database: '{db_name}'",
            rel_type="HAS_SCHEMA",
            rel_properties="type: 'has_schema'"
        )
        success, result = self.executor.execute_transactional_cypher(cypher)
        return success
    
    def create_has_table_relationship(self, schema_name: str, table_name: str, db_name: str) -> bool:
        """创建模式拥有表的关系"""
        cypher = templates.create_relationship.format(
            label1="Schema",
            match1=f"name: '{schema_name}', database: '{db_name}'",
            label2="Table",
            match2=f"name: '{table_name}', schema: '{schema_name}'",
            rel_type="HAS_TABLE",
            rel_properties="type: 'has_table'"
        )
        success, result = self.executor.execute_transactional_cypher(cypher)
        return success
    
    def create_uses_field_group_relationship(self, table_name: str, group_name: str, schema: str) -> bool:
        """创建表使用字段组的关系"""
        cypher = templates.create_relationship.format(
            label1="Table",
            match1=f"name: '{table_name}', schema: '{schema}'",
            label2="SharedFieldGroup",
            match2=f"name: '{group_name}', schema: '{schema}'",
            rel_type="USES_FIELD_GROUP",
            rel_properties="type: 'uses_field_group'"
        )
        success, result = self.executor.execute_transactional_cypher(cypher)
        return success
    
    def create_group_has_field_relationship(self, group_name: str, field_name: str, schema: str) -> bool:
        """创建字段组拥有字段的关系（确保字段组和字段的精确匹配）"""
        try:
            # 转义特殊字符
            escaped_group_name = self._escape_string(group_name)
            escaped_field_name = self._escape_string(field_name)
            escaped_schema = self._escape_string(schema)
            
            # 确保只匹配属于该字段组的字段节点
            cypher = templates.create_relationship.format(
                label1="SharedFieldGroup",
                match1=f"name: '{escaped_group_name}', schema: '{escaped_schema}'",
                label2="Field",
                match2=f"name: '{escaped_field_name}', schema: '{escaped_schema}', field_group: '{escaped_group_name}', node_type: 'shared_field'",
                rel_type="HAS_FIELD",
                rel_properties="type: 'has_field'"
            )
            success, result = self.executor.execute_transactional_cypher(cypher)
            if not success:
                logger.error(f"RelationshipCreator: HAS_FIELD关系创建失败: {group_name} -> {field_name}")
            return success
        except Exception as e:
            logger.error(f"RelationshipCreator: HAS_FIELD关系创建异常: {group_name} -> {field_name} - {str(e)}")
            return False
    
    def _escape_string(self, text: str) -> str:
        """超强力字符串转义方法，与NodeCreator保持一致"""
        if not text:
            return ""
        
        # 转换为字符串并限制长度
        cleaned_text = str(text)
        if len(cleaned_text) > 500:
            cleaned_text = cleaned_text[:500] + "..."
        
        # 第一步：移除所有控制字符和不可见字符
        import re
        # 移除所有ASCII控制字符 (0-31) 和 DEL (127)
        cleaned_text = re.sub(r'[\x00-\x1f\x7f]', ' ', cleaned_text)
        
        # 第二步：转义所有可能导致问题的字符
        # 使用更激进的转义策略
        cleaned_text = cleaned_text.replace('\\', '\\\\')  # 反斜杠
        cleaned_text = cleaned_text.replace("'", "\\'")    # 单引号
        cleaned_text = cleaned_text.replace('"', '\\"')    # 双引号
        
        # 第三步：确保没有任何换行符残留
        cleaned_text = cleaned_text.replace('\n', ' ')     # 换行符
        cleaned_text = cleaned_text.replace('\r', ' ')     # 回车符
        cleaned_text = cleaned_text.replace('\t', ' ')     # 制表符
        
        # 第四步：处理Unicode换行符
        cleaned_text = cleaned_text.replace('\u2028', ' ')  # 行分隔符
        cleaned_text = cleaned_text.replace('\u2029', ' ')  # 段落分隔符
        
        # 第五步：清理多余的空格
        cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
        
        # 第六步：最后安全检查 - 如果仍包含问题字符，直接移除
        if any(ord(c) < 32 for c in cleaned_text):
            cleaned_text = ''.join(c if ord(c) >= 32 else ' ' for c in cleaned_text)
            cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
        
        return cleaned_text
    
    def create_table_has_field_relationship(self, table_name: str, field_name: str, schema: str, field_key: str) -> bool:
        """创建表直接拥有字段的关系（用于独有字段）"""
        # 使用表名进行精确匹配，确保只连接到属于该表的独有字段节点
        # 通过table属性确保字段与表的精确对应关系
        cypher = templates.create_relationship.format(
            label1="Table",
            match1=f"name: '{table_name}', schema: '{schema}'",
            label2="Field",
            match2=f"name: '{field_name}', schema: '{schema}', table: '{table_name}', node_type: 'unique_field'",
            rel_type="HAS_UNIQUE_FIELD",
            rel_properties="type: 'has_unique_field'"
        )
        success, result = self.executor.execute_transactional_cypher(cypher)
        if success:
            logger.debug(f"RelationshipCreator: 创建表-字段关系: {table_name} -> {field_name}")
        else:
            logger.error(f"RelationshipCreator: 创建表-字段关系失败: {table_name} -> {field_name}")
        return success 