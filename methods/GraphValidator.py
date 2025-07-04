"""
图验证器模块
负责验证图数据的完整性和提供统计信息
"""
import sys
import os
import logging
from typing import Dict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.CypherExecutor import CypherExecutor

# 创建带有模块名的logger
logger = logging.getLogger(__name__)


class GraphValidator:
    """图验证器类，负责验证图数据完整性和提供统计信息"""
    
    def __init__(self, executor: CypherExecutor):
        """
        初始化图验证器
        Args:
            executor: Cypher执行器实例
        """
        self.executor = executor
    
    def validate_graph_integrity(self) -> bool:
        """验证图的完整性，检查可能的建模问题"""
        logger.info("GraphValidator: 开始验证图数据完整性...")
        
        issues_found = []
        
        # 1. 检查是否有只被一个表使用的SharedFieldGroup
        single_use_groups_cypher = """
        MATCH (sfg:SharedFieldGroup)<-[:USES_FIELD_GROUP]-(t:Table)
        WITH sfg, COUNT(t) as table_count
        WHERE table_count = 1
        RETURN sfg.name AS group_name, table_count
        """
        success, result = self.executor.execute_transactional_cypher(single_use_groups_cypher)
        if success and result:
            for record in result:
                issues_found.append(f"⚠️  共享字段组 '{record['group_name']}' 只被 1 个表使用")
        
        # 2. 检查是否有SharedFieldGroup没有Field节点
        empty_groups_cypher = """
        MATCH (sfg:SharedFieldGroup)
        WHERE NOT EXISTS((sfg)-[:HAS_FIELD]->(:Field))
        RETURN sfg.name AS group_name
        """
        success, result = self.executor.execute_transactional_cypher(empty_groups_cypher)
        if success and result:
            for record in result:
                issues_found.append(f"❌ 共享字段组 '{record['group_name']}' 没有字段节点")
        
        # 2.1 检查是否有SharedFieldGroup字段数量小于2（不符合"组"的定义）
        insufficient_fields_groups_cypher = """
        MATCH (sfg:SharedFieldGroup)-[:HAS_FIELD]->(f:Field)
        WITH sfg, COUNT(f) as field_count
        WHERE field_count < 2
        RETURN sfg.name AS group_name, field_count
        """
        success, result = self.executor.execute_transactional_cypher(insufficient_fields_groups_cypher)
        if success and result:
            for record in result:
                issues_found.append(f"❌ 共享字段组 '{record['group_name']}' 只有 {record['field_count']} 个字段，不符合群组定义")
        
        # 3. 检查是否有表没有任何字段连接
        tables_without_fields_cypher = """
        MATCH (t:Table)
        WHERE NOT EXISTS((t)-[:USES_FIELD_GROUP]->(:SharedFieldGroup)) 
          AND NOT EXISTS((t)-[:HAS_UNIQUE_FIELD]->(:Field))
        RETURN t.name AS table_name
        """
        success, result = self.executor.execute_transactional_cypher(tables_without_fields_cypher)
        if success and result:
            for record in result:
                issues_found.append(f"❌ 表 '{record['table_name']}' 没有字段连接")
        
        # 4. 检查字段是否有孤立的Field节点（没有被任何组或表引用）
        orphaned_fields_cypher = """
        MATCH (f:Field)
        WHERE NOT EXISTS((:SharedFieldGroup)-[:HAS_FIELD]->(f)) 
          AND NOT EXISTS((:Table)-[:HAS_UNIQUE_FIELD]->(f))
        RETURN f.name AS field_name, f.schema AS schema, f.node_type AS node_type
        """
        success, result = self.executor.execute_transactional_cypher(orphaned_fields_cypher)
        if success and result:
            for record in result:
                node_type = record.get('node_type', 'unknown')
                issues_found.append(f"❌ 孤立字段 '{record['schema']}.{record['field_name']}' ({node_type}) 没有被引用")
        
        # 4.1 检查是否有字段同时被SharedFieldGroup和Table直接引用（重复连接）
        duplicate_references_cypher = """
        MATCH (f:Field)<-[:HAS_FIELD]-(sfg:SharedFieldGroup),
              (f)<-[:HAS_UNIQUE_FIELD]-(t:Table)
        RETURN f.name AS field_name, f.schema AS schema, f.node_type AS node_type,
               sfg.name AS group_name, t.name AS table_name
        """
        success, result = self.executor.execute_transactional_cypher(duplicate_references_cypher)
        if success and result:
            for record in result:
                issues_found.append(f"❌ 字段 '{record['schema']}.{record['field_name']}' 同时被共享字段组 '{record['group_name']}' 和表 '{record['table_name']}' 引用")
        
        # 4.2 检查是否有字段被多个SharedFieldGroup引用（违反独立性原则）
        multi_group_references_cypher = """
        MATCH (f:Field)<-[:HAS_FIELD]-(sfg:SharedFieldGroup)
        WITH f, COUNT(sfg) as group_count, COLLECT(sfg.name) as group_names
        WHERE group_count > 1
        RETURN f.name AS field_name, f.schema AS schema, f.field_group AS expected_group, 
               group_count, group_names
        """
        success, result = self.executor.execute_transactional_cypher(multi_group_references_cypher)
        if success and result:
            for record in result:
                field_name = record['field_name']
                schema = record['schema']
                expected_group = record.get('expected_group', 'unknown')
                group_count = record['group_count']
                group_names = record['group_names']
                issues_found.append(f"❌ 字段 '{schema}.{field_name}' 被 {group_count} 个字段组引用: {group_names}，但应该只属于: {expected_group}")
        
        # 报告验证结果
        if issues_found:
            logger.warning(f"GraphValidator: 发现 {len(issues_found)} 个潜在问题：")
            for issue in issues_found:
                logger.warning(f"GraphValidator: {issue}")
            return False
        else:
            logger.info("GraphValidator: 图数据完整性验证通过")
            return True
    
    def get_graph_statistics(self) -> Dict[str, int]:
        """获取图统计信息（包含共享字段组建模）"""
        stats = {}
        
        # 统计各种节点类型
        node_types = ["Database", "Schema", "Table", "Column", "SharedFieldGroup", "Field"]
        for node_type in node_types:
            count_cypher = f"MATCH (n:{node_type}) RETURN COUNT(n) AS count"
            success, result = self.executor.execute_transactional_cypher(count_cypher)
            if success and result:
                stats[node_type] = result[0].get('count', 0)
            else:
                stats[node_type] = 0
        
        # 统计各种关系数量
        relationship_types = ["HAS_SCHEMA", "HAS_TABLE", "USES_FIELD_GROUP", "HAS_FIELD", "HAS_UNIQUE_FIELD"]
        for rel_type in relationship_types:
            rel_cypher = f"MATCH ()-[r:{rel_type}]->() RETURN COUNT(r) AS count"
            success, result = self.executor.execute_transactional_cypher(rel_cypher)
            if success and result:
                stats[f'{rel_type}_Relationships'] = result[0].get('count', 0)
            else:
                stats[f'{rel_type}_Relationships'] = 0
        
        # 总关系数
        total_rel_cypher = "MATCH ()-[r]->() RETURN COUNT(r) AS count"
        success, result = self.executor.execute_transactional_cypher(total_rel_cypher)
        if success and result:
            stats['Total_Relationships'] = result[0].get('count', 0)
        else:
            stats['Total_Relationships'] = 0
        
        return stats
    
    def print_graph_summary(self):
        """打印图摘要信息（共享字段组建模）"""
        logger.info("=== 图数据库摘要 (共享字段组建模) ===")
        stats = self.get_graph_statistics()
        
        # 统计不同类型的字段
        shared_fields_cypher = "MATCH (f:Field) WHERE f.node_type = 'shared_field' RETURN COUNT(f) AS count"
        success, result = self.executor.execute_transactional_cypher(shared_fields_cypher)
        shared_fields_count = result[0].get('count', 0) if success and result else 0
        
        unique_fields_cypher = "MATCH (f:Field) WHERE f.node_type = 'unique_field' RETURN COUNT(f) AS count"
        success, result = self.executor.execute_transactional_cypher(unique_fields_cypher)
        unique_fields_count = result[0].get('count', 0) if success and result else 0
        
        logger.info("GraphValidator: 节点统计:")
        logger.info(f"GraphValidator:   数据库: {stats.get('Database', 0)}")
        logger.info(f"GraphValidator:   模式: {stats.get('Schema', 0)}")
        logger.info(f"GraphValidator:   表: {stats.get('Table', 0)}")
        logger.info(f"GraphValidator:   共享字段组: {stats.get('SharedFieldGroup', 0)}")
        logger.info(f"GraphValidator:   共享字段: {shared_fields_count}")
        logger.info(f"GraphValidator:   独有字段: {unique_fields_count}")
        logger.info(f"GraphValidator:   字段总数: {stats.get('Field', 0)}")
        logger.info(f"GraphValidator:   传统列: {stats.get('Column', 0)}")
        
        logger.info("GraphValidator: 关系统计:")
        logger.info(f"GraphValidator:   数据库拥有模式关系: {stats.get('HAS_SCHEMA_Relationships', 0)}")
        logger.info(f"GraphValidator:   模式拥有表关系: {stats.get('HAS_TABLE_Relationships', 0)}")
        logger.info(f"GraphValidator:   表使用字段组关系: {stats.get('USES_FIELD_GROUP_Relationships', 0)}")
        logger.info(f"GraphValidator:   字段组拥有字段关系: {stats.get('HAS_FIELD_Relationships', 0)}")
        logger.info(f"GraphValidator:   表拥有独有字段关系: {stats.get('HAS_UNIQUE_FIELD_Relationships', 0)}")
        logger.info(f"GraphValidator:   总关系数: {stats.get('Total_Relationships', 0)}")
        
        # 显示一些示例查询结果
        logger.info("GraphValidator: === 示例查询 ===")
        
        # 查询所有数据库
        db_cypher = "MATCH (d:Database) RETURN d.name AS database ORDER BY d.name"
        success, result = self.executor.execute_transactional_cypher(db_cypher)
        if success and result:
            logger.info("GraphValidator: 数据库列表:")
            for record in result:
                logger.info(f"GraphValidator:   - {record['database']}")
        
        # 查询共享字段组及其使用的表
        shared_groups_cypher = """
        MATCH (sfg:SharedFieldGroup)<-[:USES_FIELD_GROUP]-(t:Table)
        RETURN sfg.name AS field_group, 
               COUNT(t) AS table_count,
               COLLECT(t.name)[..5] AS sample_tables
        ORDER BY table_count DESC
        LIMIT 5
        """
        success, result = self.executor.execute_transactional_cypher(shared_groups_cypher)
        if success and result:
            logger.info("GraphValidator: 共享字段组及其使用表:")
            for record in result:
                group = record['field_group']
                count = record['table_count']
                samples = record['sample_tables']
                logger.info(f"GraphValidator:   {group}: {count} 个表使用")
                for sample in samples:
                    logger.info(f"GraphValidator:     - {sample}")
        
        # 查询字段组的字段分布
        field_group_distribution_cypher = """
        MATCH (sfg:SharedFieldGroup)-[:HAS_FIELD]->(f:Field)
        RETURN sfg.name AS field_group, COUNT(f) AS field_count
        ORDER BY field_count DESC
        LIMIT 5
        """
        success, result = self.executor.execute_transactional_cypher(field_group_distribution_cypher)
        if success and result:
            logger.info("GraphValidator: 字段组字段分布:")
            for record in result:
                logger.info(f"GraphValidator:   {record['field_group']}: {record['field_count']} 个字段")
        
        # 查询独有字段的表
        unique_field_tables_cypher = """
        MATCH (t:Table)-[:HAS_UNIQUE_FIELD]->(f:Field)
        RETURN t.name AS table_name, COUNT(f) AS unique_field_count
        ORDER BY unique_field_count DESC
        LIMIT 5
        """
        success, result = self.executor.execute_transactional_cypher(unique_field_tables_cypher)
        if success and result:
            logger.info("GraphValidator: 拥有独有字段的表:")
            for record in result:
                logger.info(f"GraphValidator:   {record['table_name']}: {record['unique_field_count']} 个独有字段")
        
        logger.info("GraphValidator: 建模摘要:")
        table_count = stats.get('Table', 0)
        field_group_count = stats.get('SharedFieldGroup', 0)
        field_count = stats.get('Field', 0)
        
        if table_count > 0 and field_group_count > 0:
            # 计算字段复用率
            uses_group_relationships = stats.get('USES_FIELD_GROUP_Relationships', 0)
            unique_field_relationships = stats.get('HAS_UNIQUE_FIELD_Relationships', 0)
            
            if uses_group_relationships > 0:
                avg_tables_per_group = uses_group_relationships / field_group_count
                logger.info(f"GraphValidator:   平均每个字段组被 {avg_tables_per_group:.1f} 个表使用")
            
            logger.info(f"GraphValidator:   字段组数量: {field_group_count}")
            logger.info(f"GraphValidator:   使用共享字段组的表关系: {uses_group_relationships}")
            logger.info(f"GraphValidator:   独有字段关系: {unique_field_relationships}")
            
            # 计算字段复用效果
            if field_count > 0:
                shared_field_ratio = stats.get('HAS_FIELD_Relationships', 0) / field_count * 100
                logger.info(f"GraphValidator:   字段共享率: {shared_field_ratio:.1f}%") 