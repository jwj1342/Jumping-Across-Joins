import sys
import os
import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.CypherExecutor import CypherExecutor
from methods.NodeCreator import NodeCreator
from methods.RelationshipCreator import RelationshipCreator
from methods.GraphValidator import GraphValidator
from methods.GraphUtils import GraphUtils, FieldGroupOptimizer

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GraphBuilder:
    def __init__(self):
        """初始化图构建器"""
        self.executor = CypherExecutor()
        self.database_root = "resource/databases"
        self.field_groups = {}  # 存储字段组信息 {field_group_hash: field_group_info}
        self.all_fields = {}   # 存储所有字段信息 {field_name_type: field_info}
        
        # 初始化各个功能模块
        self.node_creator = NodeCreator(self.executor)
        self.relationship_creator = RelationshipCreator(self.executor)
        self.validator = GraphValidator(self.executor)
        self.utils = GraphUtils()
        self.field_group_optimizer = FieldGroupOptimizer()
        
    def clear_existing_graph(self):
        """清理现有图数据"""
        logger.info("清理现有图数据...")
        clear_cypher = "MATCH (n) DETACH DELETE n"
        success, result = self.executor.execute_transactional_cypher(clear_cypher)
        if success:
            logger.info("现有图数据已清理")
        else:
            logger.error("清理图数据失败")
        return success
    
    def build_database_graph(self, db_name: str) -> bool:
        """为指定数据库构建图（支持共享字段组建模）"""
        logger.info(f"=== 开始构建数据库图: {db_name} (共享字段组模式) ===")
        
        db_path = os.path.join(self.database_root, db_name)
        if not os.path.exists(db_path):
            logger.error(f"数据库目录不存在: {db_path}")
            return False
        
        # 创建数据库节点
        if not self.node_creator.create_database_node(db_name):
            return False
        
        # 第一阶段：分析所有表结构，识别字段组
        logger.info("第一阶段：分析表字段结构...")
        field_groups = defaultdict(list)  # {field_hash: [(table_info, schema_name, json_file), ...]}
        all_tables = []  # 存储所有表信息用于后续处理
        
        schema_dirs = [d for d in os.listdir(db_path) if os.path.isdir(os.path.join(db_path, d))]
        
        for schema_name in schema_dirs:
            schema_path = os.path.join(db_path, schema_name)
            json_files = [f for f in os.listdir(schema_path) if f.endswith('.json')]
            
            for json_file in json_files:
                json_path = os.path.join(schema_path, json_file)
                try:
                    with open(json_path, 'r', encoding='utf-8') as f:
                        table_info = json.load(f)
                    
                    table_name = table_info.get('table_name', '')
                    if not table_name:
                        continue
                    
                    column_names = table_info.get('column_names', [])
                    column_types = table_info.get('column_types', [])
                    
                    if column_names:  # 只处理有字段的表
                        field_hash = self.utils.calculate_field_group_hash(column_names, column_types)
                        field_groups[field_hash].append((table_info, schema_name, json_file))
                        all_tables.append((table_info, schema_name, json_file, field_hash))
                        
                except Exception as e:
                    logger.error(f"分析表文件失败 {json_file}: {e}")
                    continue
        
        # 第二阶段：创建模式和优化字段组
        logger.info(f"第二阶段：优化并创建共享字段组...")
        logger.info(f"发现 {len(field_groups)} 种不同的字段组合")
        
        # 创建所有模式节点
        for schema_name in schema_dirs:
            if not self.node_creator.create_schema_node(db_name, schema_name):
                continue
            
            # 创建数据库->模式的关系
            self.relationship_creator.create_has_schema_relationship(db_name, schema_name)
        
        # 使用优化器选择最佳字段组集合（精确匹配模式）
        optimized_field_groups = self.field_group_optimizer.optimize_field_groups_with_exact_matching(field_groups)
        
        # 为优化后的字段组创建SharedFieldGroup节点
        shared_field_groups = {}  # {field_hash: group_name}
        
        for field_hash, group_info in optimized_field_groups.items():
            group_name = group_info['group_name']
            schema = group_info['schema']
            column_names = group_info['column_names']
            column_types = group_info['column_types']
            table_count = group_info['table_count']
            field_count = group_info['field_count']
            tables = group_info['tables']
            
            shared_field_groups[field_hash] = group_name
            
            logger.info(f"创建优化字段组 {field_hash[:8]}... 包含 {table_count} 个表:")
            for table_name in tables[:5]:  # 只显示前5个表
                logger.info(f"    - {table_name}")
            if len(tables) > 5:
                logger.info(f"    ... 还有 {len(tables) - 5} 个表")
            logger.info(f"    字段组名: {group_name}")
            logger.info(f"    字段组合: {column_names}")
            logger.info(f"    字段类型: {column_types}")
            
            # 创建共享字段组节点
            if self.node_creator.create_shared_field_group_node(group_name, db_name, schema, 
                                                 field_hash, field_count):
                
                # 为字段组创建字段节点和关系
                logger.debug(f"    为字段组创建字段...")
                
                # 获取代表性表的描述信息
                representative_table_info = None
                for table_info, table_schema, _ in field_groups[field_hash]:
                    if table_schema == schema:
                        representative_table_info = table_info
                        break
                
                descriptions = representative_table_info.get('description', []) if representative_table_info else []
                sample_rows = representative_table_info.get('sample_rows', []) if representative_table_info else []
                
                for i, col_name in enumerate(column_names):
                    col_type = column_types[i] if i < len(column_types) else "UNKNOWN"
                    description = descriptions[i] if i < len(descriptions) and descriptions[i] else ""
                    sample_data = self.utils.extract_sample_data(sample_rows, col_name)
                    
                    # 为共享字段组创建字段节点（每个字段组有独立的字段实例）
                    field_key = f"{group_name}.{col_name}:{col_type}:shared"
                    if field_key not in self.all_fields:
                        # 创建字段节点（包含字段组标识，确保独立性）
                        node_created = self.node_creator.create_shared_field_node(col_name, col_type, db_name, schema, group_name, description, sample_data)
                        if node_created:
                            # 节点创建成功，创建关系
                            rel_created = self.relationship_creator.create_group_has_field_relationship(group_name, col_name, schema)
                            if rel_created:
                                self.all_fields[field_key] = True
                                logger.debug(f"      字段组关系: {group_name} -> {col_name}")
                            else:
                                logger.error(f"      字段组关系创建失败: {group_name} -> {col_name}")
                        else:
                            logger.error(f"      字段节点创建失败: {col_name} ({col_type}) - 可能包含特殊字符")
                    else:
                        logger.warning(f"      字段已存在，跳过: {col_name}")
                
                # 记录这个字段组信息
                self.field_groups[field_hash] = group_info
        
        # 字段组创建完成后的摘要信息
        logger.info(f"字段组创建摘要:")
        logger.info(f"  共创建 {len(self.field_groups)} 个共享字段组")
        for field_hash, group_info in self.field_groups.items():
            group_name = group_info['group_name']
            column_names = group_info['column_names']
            logger.info(f"  - {group_name} (哈希: {field_hash[:8]}...)")
            logger.info(f"    字段: {column_names}")
        
        # 第三阶段：创建表节点和关系
        logger.info(f"第三阶段：创建表节点和关系...")
        
        for schema_name in schema_dirs:
            schema_path = os.path.join(db_path, schema_name)
            logger.info(f"--- 处理模式: {schema_name} ---")
            
            # 处理该模式下的所有表
            schema_tables = [t for t in all_tables if t[1] == schema_name]
            
            for table_info, _, json_file, field_hash in schema_tables:
                try:
                    table_name = table_info.get('table_name', '')
                    if not table_name:
                        continue
                    
                    column_names = table_info.get('column_names', [])
                    column_types = table_info.get('column_types', [])
                    
                    # 获取DDL信息
                    ddl_file = os.path.join(schema_path, "DDL.csv")
                    ddl_info = self.utils.load_ddl_info(ddl_file)
                    ddl = ddl_info.get(table_name.split('.')[-1], "")
                    
                    # 创建表节点
                    logger.debug(f"  处理表: {table_name}")
                    if not self.node_creator.create_table_node(db_name, schema_name, table_info, ddl):
                        continue
                    
                    # 创建模式->表的关系
                    self.relationship_creator.create_has_table_relationship(schema_name, table_name, db_name)
                    
                    # 处理表的字段关系（支持混合模式：共享字段组 + 独有字段）
                    shared_fields_count, unique_fields_count = self.create_table_field_relationships_mixed_mode(table_info, table_name, schema_name, db_name)
                    
                    # 显示字段关系汇总
                    total_fields = len(column_names)
                    if shared_fields_count > 0 and unique_fields_count > 0:
                        logger.info(f"    混合模式: {shared_fields_count} 个共享字段, {unique_fields_count} 个独有字段")
                    elif shared_fields_count > 0:
                        logger.info(f"    纯共享模式: {shared_fields_count} 个字段通过共享字段组")
                    elif unique_fields_count > 0:
                        logger.info(f"    纯独有模式: {unique_fields_count} 个独有字段")
                    else:
                        logger.warning(f"    警告: 表 {table_name} 没有建立任何字段关系")
                
                except Exception as e:
                    logger.error(f"  处理表文件失败 {json_file}: {e}")
                    continue
        
        # 第四阶段：验证建模完整性
        logger.info(f"第四阶段：验证建模完整性...")
        self.validator.validate_graph_integrity()
        
        logger.info(f"数据库图构建完成: {db_name}")
        return True
    
    # 验证和统计方法已迁移到GraphValidator模块
    
    def close(self):
        """关闭数据库连接"""
        if self.executor:
            self.executor.close()

    # find_field_in_shared_groups方法已迁移到GraphUtils模块（需要传递field_groups参数）
    
    def create_table_field_relationships_mixed_mode(self, table_info: Dict[str, Any], 
                                                   table_name: str, schema_name: str, 
                                                   db_name: str) -> Tuple[int, int]:
        """
        为表创建字段关系（精确匹配模式）
        返回 (共享字段关系数, 独有字段关系数)
        """
        column_names = table_info.get('column_names', [])
        column_types = table_info.get('column_types', [])
        descriptions = table_info.get('description', [])
        sample_rows = table_info.get('sample_rows', [])
        
        shared_fields_count = 0
        unique_fields_count = 0
        
        logger.info(f"    -> 精确匹配字段关系 ({len(column_names)} 个字段)")
        
        # 构建表的字段列表
        table_fields = []
        for i, col_name in enumerate(column_names):
            col_type = column_types[i] if i < len(column_types) else "UNKNOWN"
            table_fields.append((col_name, col_type))
        
        # 尝试找到与表字段集合完全匹配的字段组
        exact_matching_group = self.utils.find_exact_matching_field_group(table_fields, schema_name, self.field_groups)
        
        if exact_matching_group:
            # 找到精确匹配的字段组，创建表->字段组关系
            logger.info(f"    ✓ 表 {table_name} 精确匹配到字段组: {exact_matching_group}")
            logger.info(f"      表字段集合: {[f'{name}:{type_}' for name, type_ in table_fields]}")
            
            # 创建USES_FIELD_GROUP关系
            self.relationship_creator.create_uses_field_group_relationship(table_name, exact_matching_group, schema_name)
            shared_fields_count = len(column_names)
            
        else:
            # 没有找到精确匹配的字段组，所有字段都作为独有字段处理
            logger.info(f"    ✗ 表 {table_name} 没有找到精确匹配的字段组，创建独有字段")
            logger.info(f"      表字段集合: {[f'{name}:{type_}' for name, type_ in table_fields]}")
            
            for i, col_name in enumerate(column_names):
                col_type = column_types[i] if i < len(column_types) else "UNKNOWN"
                description = descriptions[i] if i < len(descriptions) and descriptions[i] else ""
                sample_data = self.utils.extract_sample_data(sample_rows, col_name)
                
                # 创建独有字段
                field_key = f"{schema_name}.{col_name}:{col_type}:{table_name}"
                if field_key not in self.all_fields:
                    if self.node_creator.create_field_node(col_name, col_type, db_name, schema_name, table_name, description, sample_data):
                        self.all_fields[field_key] = True
                        # 创建表->独有字段的关系
                        self.relationship_creator.create_table_has_field_relationship(table_name, col_name, schema_name, field_key)
                        logger.debug(f"      创建独有字段: {col_name} ({col_type}) -> {table_name}")
                        unique_fields_count += 1
        
        return shared_fields_count, unique_fields_count

def main():
    """主函数 - 构建指定数据库的图（共享字段组建模）"""
    import argparse
    
    parser = argparse.ArgumentParser(description='数据库结构图构建程序（共享字段组建模）')
    parser.add_argument('database', help='要构建的数据库名称（例如：NORTHWIND、BLS）')
    parser.add_argument('--clear', action='store_true', help='构建前清理现有图数据')
    parser.add_argument('--stats', action='store_true', help='显示构建后的统计信息')
    
    args = parser.parse_args()
    
    logger.info("数据库结构图构建程序")
    logger.info("=" * 60)
    logger.info(f"目标数据库: {args.database}")
    
    builder = GraphBuilder()
    
    try:
        # 验证数据库连接
        if not builder.executor.verify_connectivity():
            logger.error("数据库连接失败，程序退出")
            return
        
        logger.info("数据库连接成功")
        
        # 可选清理现有数据
        if args.clear:
            builder.clear_existing_graph()
        
        # 构建图
        success = builder.build_database_graph(args.database)
        
        if success and args.stats:
            builder.validator.print_graph_summary()
            
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"程序执行过程中发生错误: {e}")
        logging.exception("详细错误信息:")
    finally:
        builder.close()
        logger.info("程序结束，数据库连接已关闭")


if __name__ == "__main__":
    # 如果没有命令行参数，默认使用BLS进行测试
    if len(sys.argv) == 1:
        logger.info("=" * 60)
        logger.info("默认构建: BLS")
        
        builder = GraphBuilder()
        
        try:
            # 验证数据库连接
            if not builder.executor.verify_connectivity():
                logger.error("数据库连接失败，程序退出")
                exit(1)
            
            logger.info("数据库连接成功")
            
            # 清理现有数据
            builder.clear_existing_graph()
            
            # 构建图
            success = builder.build_database_graph("BLS")
            
            if success:
                builder.validator.print_graph_summary()
                
        except KeyboardInterrupt:
            logger.info("程序被用户中断")
        except Exception as e:
            logger.error(f"程序执行过程中发生错误: {e}")
            logging.exception("详细错误信息:")
        finally:
            builder.close()
            logger.info("程序结束，数据库连接已关闭")
    else:
        main()
