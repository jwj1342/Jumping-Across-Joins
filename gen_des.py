import os
import json
import logging
import numpy as np
from typing import List, Dict, Tuple, Optional
from pathlib import Path
from dotenv import load_dotenv
import faiss
from openai import OpenAI
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.CypherExecutor import CypherExecutor
from utils.init_llm import initialize_llm

# 一些Cypher模板
## 获取所有的需要生成描述数据的两类节点信息
GET_COUNT_OF_DESCRIPTION = """
MATCH (db:Database {name: $database_name})

// 统计standalone表
OPTIONAL MATCH (db)-[:HAS_SCHEMA]->(:Schema)-[:HAS_TABLE]->(t:Table)
WHERE NOT (t)-[:USES_FIELD_GROUP]->(:SharedFieldGroup)
WITH db, 
     COUNT(t) as total_standalone_tables,
     COUNT(CASE WHEN t.description IS NOT NULL AND t.description <> '' THEN t END) as described_standalone_tables

// 统计SharedFieldGroup
OPTIONAL MATCH (db)-[:HAS_SCHEMA]->(:Schema)-[:HAS_TABLE]->(:Table)-[:USES_FIELD_GROUP]->(sfg:SharedFieldGroup)
WITH db, total_standalone_tables, described_standalone_tables,
     COUNT(DISTINCT sfg) as total_shared_groups,
     COUNT(DISTINCT CASE WHEN sfg.description IS NOT NULL AND sfg.description <> '' 
                        THEN sfg END) as described_shared_groups

RETURN db.name AS database_name,
       total_standalone_tables,
       described_standalone_tables,
       total_shared_groups,
       described_shared_groups
"""

## 为standalone_table添加描述属性
SET_DESCRIPTION_FOR_STANDALONE_TABLE = """
MATCH (t:Table)
WHERE NOT (t)-[:USES_FIELD_GROUP]->(:SharedFieldGroup)
  AND t.name = $table_name  // 可选过滤条件

SET t.description = $description
RETURN t.name, t.description
"""

## 为shared_field_group添加描述属性
SET_DESCRIPTION_FOR_SHARED_FIELD_GROUP = """
MATCH (g:SharedFieldGroup)
WHERE g.name = $group_name  // 可选过滤条件

SET g.description = $description
RETURN g.name, g.description
"""

## 获取一个表下面的所有字段信息
GET_FIELDS_OF_TABLE = """
MATCH (t:Table {name: $table_name})
OPTIONAL MATCH (t)-[:HAS_UNIQUE_FIELD]->(f:Field)
RETURN t.name AS table_name, f.name AS field_name, f.type AS field_type, f.description AS field_description
ORDER BY f.name
"""

## 获取一个shared_field_group连接的表和拥有的字段
GET_SHARED_GROUP_USAGE = """
MATCH (g:SharedFieldGroup {name: $group_name})
OPTIONAL MATCH (t:Table)-[:USES_FIELD_GROUP]->(g)
OPTIONAL MATCH (g)-[:HAS_FIELD]->(f:Field)
RETURN 
  g.name AS group_name,
  COLLECT(DISTINCT t.name) AS connected_tables,
  COLLECT(DISTINCT f.name) AS shared_fields
"""

## 获取数据库下所有standalone表（没有描述的）
GET_STANDALONE_TABLES_BY_DATABASE = """
MATCH (db:Database {name: $database_name})-[:HAS_SCHEMA]->(:Schema)-[:HAS_TABLE]->(t:Table)
WHERE NOT (t)-[:USES_FIELD_GROUP]->(:SharedFieldGroup)
  AND (t.description IS NULL OR t.description = '')
RETURN t.name AS table_name, t.schema AS schema_name, t.database AS database_name
ORDER BY t.name
"""

## 获取数据库下所有SharedFieldGroup（没有描述的）
GET_SHARED_FIELD_GROUPS_BY_DATABASE = """
MATCH (db:Database {name: $database_name})-[:HAS_SCHEMA]->(:Schema)-[:HAS_TABLE]->(:Table)-[:USES_FIELD_GROUP]->(sfg:SharedFieldGroup)
WHERE sfg.description IS NULL OR sfg.description = ''
RETURN DISTINCT sfg.name AS group_name
ORDER BY sfg.name
"""

## 查询所有的数据库
GET_ALL_DATABASES = """
MATCH (db:Database)
RETURN db.name AS database_name
ORDER BY db.name
"""

class GenerateDescription:
    """
    描述生成器
    对于生成的描述数据，作用的节点主要是一个schema下面的所有的table上面，但是需要排除连接了SharedFieldGroup的Table
    对于生成的描述数据，作用的第二个节点就是所有的SharedFieldGroup node。
    """
    
    def __init__(self, enable_info_logging=False):
        """
        初始化描述生成器
        
        Args:
            enable_info_logging (bool): 是否启用info级别日志
        """
        self.enable_info_logging = enable_info_logging
        
        # 创建独立的logger
        self.logger = logging.getLogger(f"{__name__}.GenerateDescription")
        self.setup_logging()
        
        # 加载环境变量
        load_dotenv(".env")
        
        # 初始化LLM
        self.llm = initialize_llm()
        if not self.llm:
            self.logger.error("LLM初始化失败")
            
        # 初始化数据库连接
        self.cypher_executor = CypherExecutor(enable_info_logging=enable_info_logging)
        
        # 设置线程池
        self.executor = ThreadPoolExecutor(max_workers=10)
        
    def setup_logging(self):
        """设置独立的日志配置"""
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            
        # 只保留ERROR级别的日志
        self.logger.setLevel(logging.ERROR)
        self.logger.propagate = False
    
    def _log_info(self, message: str):
        """条件性记录debug日志 - 现在只记录error"""
        pass  # 不再输出DEBUG日志
    
    def get_database_description_count(self, database_name: str) -> Dict[str, int]:
        """
        获取指定数据库需要生成描述的节点数量统计
        
        Args:
            database_name (str): 数据库名称
            
        Returns:
            Dict[str, int]: 包含各类统计数据的字典
        """
        parameters = {"database_name": database_name}
        success, results = self.cypher_executor.execute_transactional_cypher(
            GET_COUNT_OF_DESCRIPTION, parameters
        )
        
        if not success or not results:
            self.logger.error(f"获取数据库 {database_name} 描述统计失败")
            return {
                "total_standalone_tables": 0,
                "described_standalone_tables": 0,
                "total_shared_groups": 0,
                "described_shared_groups": 0
            }
        
        result = results[0]
        count_info = {
            "total_standalone_tables": result.get("total_standalone_tables", 0),
            "described_standalone_tables": result.get("described_standalone_tables", 0),
            "total_shared_groups": result.get("total_shared_groups", 0),
            "described_shared_groups": result.get("described_shared_groups", 0)
        }
        
        self._log_info(
            f"数据库 {database_name} 统计:\n"
            f"- Standalone表: 总计 {count_info['total_standalone_tables']} 个, "
            f"已有描述 {count_info['described_standalone_tables']} 个\n"
            f"- SharedFieldGroup: 总计 {count_info['total_shared_groups']} 个, "
            f"已有描述 {count_info['described_shared_groups']} 个"
        )
        
        return count_info
    
    def get_standalone_tables(self, database_name: str) -> List[Dict]:
        """
        获取数据库下所有standalone表信息
        
        Args:
            database_name (str): 数据库名称
            
        Returns:
            List[Dict]: standalone表列表
        """
        parameters = {"database_name": database_name}
        success, results = self.cypher_executor.execute_transactional_cypher(
            GET_STANDALONE_TABLES_BY_DATABASE, parameters
        )
        
        if not success:
            self.logger.error(f"获取数据库 {database_name} standalone表失败")
            return []
        
        return results
    
    def get_shared_field_groups(self, database_name: str) -> List[Dict]:
        """
        获取数据库下所有SharedFieldGroup信息
        
        Args:
            database_name (str): 数据库名称
            
        Returns:
            List[Dict]: SharedFieldGroup列表
        """
        parameters = {"database_name": database_name}
        success, results = self.cypher_executor.execute_transactional_cypher(
            GET_SHARED_FIELD_GROUPS_BY_DATABASE, parameters
        )
        
        if not success:
            self.logger.error(f"获取数据库 {database_name} SharedFieldGroup失败")
            return []
        
        return results
    
    def get_table_fields(self, table_name: str) -> List[Dict]:
        """
        获取表的所有字段信息
        
        Args:
            table_name (str): 表名
            
        Returns:
            List[Dict]: 字段信息列表
        """
        parameters = {"table_name": table_name}
        success, results = self.cypher_executor.execute_transactional_cypher(
            GET_FIELDS_OF_TABLE, parameters
        )
        
        if not success:
            self.logger.error(f"获取表 {table_name} 字段信息失败")
            return []
        
        return results
    
    def get_shared_group_info(self, group_name: str) -> Dict:
        """
        获取SharedFieldGroup的详细信息
        
        Args:
            group_name (str): 字段组名称
            
        Returns:
            Dict: 字段组信息
        """
        parameters = {"group_name": group_name}
        success, results = self.cypher_executor.execute_transactional_cypher(
            GET_SHARED_GROUP_USAGE, parameters
        )
        
        if not success or not results:
            self.logger.error(f"获取SharedFieldGroup {group_name} 信息失败")
            return {}
        
        return results[0]
    
    def generate_table_description(self, table_name: str, table_info: Dict, fields: List[Dict]) -> str:
        """
        为standalone表生成描述
        
        Args:
            table_name (str): 表名
            table_info (Dict): 表信息
            fields (List[Dict]): 字段列表
            
        Returns:
            str: 生成的描述
        """
        if not self.llm:
            return f"表 {table_name} - LLM未初始化，无法生成描述"
        
        try:
            # 构建字段信息文本
            field_descriptions = []
            for field in fields:
                if field.get('field_name'):
                    field_type = field.get('field_type', 'unknown')
                    field_desc = f"- {field['field_name']} ({field_type})"
                    field_descriptions.append(field_desc)
            
            fields_text = "\n".join(field_descriptions) if field_descriptions else "无字段信息"
            
            # 构建提示词
            prompt = f"""
Please generate a concise description for the following database table for vector retrieval.

Table Name: {table_name}
Database: {table_info.get('database_name', 'unknown')}
Schema: {table_info.get('schema_name', 'unknown')}

Fields:
{fields_text}

Generate a concise description (no more than 200 words) that explains the main purpose and function of this table. The description should mention each field and its role. Only return the description content without any other explanations.
"""
            
            response = self.llm.invoke(prompt)
            # 正确提取内容，避免包含元数据
            if hasattr(response, 'content'):
                description = response.content.strip()
            else:
                description = str(response).strip()
            
            self._log_info(f"为表 {table_name} 生成描述: {description[:50]}...")
            return description
            
        except Exception as e:
            self.logger.error(f"生成表 {table_name} 描述失败: {e}")
            return f"表 {table_name} - 描述生成失败: {e}"
    
    def generate_shared_group_description(self, group_name: str, group_info: Dict) -> str:
        """
        为SharedFieldGroup生成描述
        
        Args:
            group_name (str): 字段组名称
            group_info (Dict): 字段组信息
            
        Returns:
            str: 生成的描述
        """
        if not self.llm:
            return f"SharedFieldGroup {group_name} - LLM未初始化，无法生成描述"
        
        try:
            connected_tables = group_info.get('connected_tables', [])
            shared_fields = group_info.get('shared_fields', [])
            
            tables_text = ", ".join(connected_tables) if connected_tables else "无连接表"
            fields_text = ", ".join(shared_fields) if shared_fields else "无字段"
            
            prompt = f"""
Please generate a concise description for the following shared field group for vector retrieval.

Field Group Name: {group_name}
Connected Tables: {tables_text}
Included Fields: {fields_text}

This is a shared field group that serves as a consolidation node for fields commonly used across multiple tables. The connected tables may represent a time series or follow a specific sequence. Please generate a concise description (max 200 words) explaining:

1. The main purpose and function of this field group
2. How these fields are shared across the connected tables
3. The role of each field: {fields_text}
4. Any potential temporal or sequential relationships between the connected tables: {tables_text}

Only return the description content without any other explanations.
"""
            
            response = self.llm.invoke(prompt)
            # 正确提取内容，避免包含元数据
            if hasattr(response, 'content'):
                description = response.content.strip()
            else:
                description = str(response).strip()
            
            self._log_info(f"为SharedFieldGroup {group_name} 生成描述: {description[:50]}...")
            return description
            
        except Exception as e:
            self.logger.error(f"生成SharedFieldGroup {group_name} 描述失败: {e}")
            return f"SharedFieldGroup {group_name} - 描述生成失败: {e}"
    
    def update_table_description(self, table_name: str, description: str) -> bool:
        """
        更新表的描述属性
        
        Args:
            table_name (str): 表名
            description (str): 描述内容
            
        Returns:
            bool: 是否成功
        """
        parameters = {"table_name": table_name, "description": description}
        success, results = self.cypher_executor.execute_transactional_cypher(
            SET_DESCRIPTION_FOR_STANDALONE_TABLE, parameters
        )
        
        if success:
            self._log_info(f"成功更新表 {table_name} 的描述")
        else:
            self.logger.error(f"更新表 {table_name} 描述失败")
        
        return success
    
    def update_shared_group_description(self, group_name: str, description: str) -> bool:
        """
        更新SharedFieldGroup的描述属性
        
        Args:
            group_name (str): 字段组名称
            description (str): 描述内容
            
        Returns:
            bool: 是否成功
        """
        parameters = {"group_name": group_name, "description": description}
        success, results = self.cypher_executor.execute_transactional_cypher(
            SET_DESCRIPTION_FOR_SHARED_FIELD_GROUP, parameters
        )
        
        if success:
            self._log_info(f"成功更新SharedFieldGroup {group_name} 的描述")
        else:
            self.logger.error(f"更新SharedFieldGroup {group_name} 描述失败")
        
        return success
    
    def process_table(self, table: Dict) -> Dict:
        """
        并发处理单个表
        
        Args:
            table (Dict): 表信息
            
        Returns:
            Dict: 处理结果
        """
        table_name = table['table_name']
        try:
            # 获取字段信息
            fields = self.get_table_fields(table_name)
            
            if not fields:
                return {"status": "failed", "name": table_name, "reason": "无字段"}
            
            # 生成描述
            description = self.generate_table_description(table_name, table, fields)
            if not description or description.startswith("表") and "失败" in description:
                return {"status": "failed", "name": table_name, "reason": "描述生成失败"}
            
            # 更新描述
            if self.update_table_description(table_name, description):
                return {"status": "success", "name": table_name}
            else:
                return {"status": "failed", "name": table_name, "reason": "数据库更新失败"}
                
        except Exception as e:
            return {"status": "failed", "name": table_name, "reason": f"异常:{str(e)}"}

    def process_shared_group(self, group: Dict) -> Dict:
        """
        并发处理单个SharedFieldGroup
        
        Args:
            group (Dict): 字段组信息
            
        Returns:
            Dict: 处理结果
        """
        group_name = group['group_name']
        try:
            # 获取组信息
            group_info = self.get_shared_group_info(group_name)
            if not group_info:
                return {"status": "failed", "name": group_name, "reason": "无信息"}
            
            # 生成描述
            description = self.generate_shared_group_description(group_name, group_info)
            if not description or description.startswith("SharedFieldGroup") and "失败" in description:
                return {"status": "failed", "name": group_name, "reason": "描述生成失败"}
            
            # 更新描述
            if self.update_shared_group_description(group_name, description):
                return {"status": "success", "name": group_name}
            else:
                return {"status": "failed", "name": group_name, "reason": "数据库更新失败"}
                
        except Exception as e:
            return {"status": "failed", "name": group_name, "reason": f"异常:{str(e)}"}

    def generate_descriptions_for_database(self, database_name: str, show_progress: bool = True) -> Tuple[bool, Dict[str, int]]:
        """
        为指定数据库生成所有描述 (并发版本)
        
        Args:
            database_name (str): 数据库名称
            show_progress (bool): 是否显示进度
            
        Returns:
            Tuple[bool, Dict[str, int]]: (是否成功, 详细统计信息)
        """
        try:
            # 获取统计信息
            count_info = self.get_database_description_count(database_name)
            total_items = (count_info['total_standalone_tables'] - count_info['described_standalone_tables'] + 
                         count_info['total_shared_groups'] - count_info['described_shared_groups'])
            
            if total_items == 0:
                return True, count_info
            
            stats = {
                "newly_described_tables": 0,
                "newly_described_groups": 0,
                "failed_tables": [],
                "failed_groups": []
            }
            
            # 获取需要处理的表和组
            tables = self.get_standalone_tables(database_name)
            groups = self.get_shared_field_groups(database_name)
            
            # 并发处理表
            table_futures = [self.executor.submit(self.process_table, table) for table in tables]
            group_futures = [self.executor.submit(self.process_shared_group, group) for group in groups]
            
            # 处理表的结果
            for future in as_completed(table_futures):
                result = future.result()
                if result["status"] == "success":
                    stats["newly_described_tables"] += 1
                else:
                    stats["failed_tables"].append(f"{result['name']}({result['reason']})")
            
            # 处理组的结果
            for future in as_completed(group_futures):
                result = future.result()
                if result["status"] == "success":
                    stats["newly_described_groups"] += 1
                else:
                    stats["failed_groups"].append(f"{result['name']}({result['reason']})")
            
            # 合并统计信息
            final_stats = {
                **count_info,
                "newly_described_tables": stats["newly_described_tables"],
                "newly_described_groups": stats["newly_described_groups"],
                "failed_tables": stats["failed_tables"],
                "failed_groups": stats["failed_groups"]
            }
            
            # 计算成功率
            needed_items = total_items
            success_items = stats["newly_described_tables"] + stats["newly_described_groups"]
            success_rate = success_items / needed_items if needed_items > 0 else 1.0
            
            is_success = success_rate >= 0.8
            if not is_success:
                self.logger.error(f"描述生成成功率 {success_rate:.1%}，未达到80%要求")
            
            return is_success, final_stats
            
        except Exception as e:
            self.logger.error(f"为数据库 {database_name} 生成描述时出错: {e}")
            return False, {}
    
    def close(self):
        """关闭连接和线程池"""
        if self.cypher_executor:
            self.cypher_executor.close()
        if self.executor:
            self.executor.shutdown(wait=True)
    
    def get_all_databases(self) -> List[str]:
        """
        获取所有数据库名称
        
        Returns:
            List[str]: 数据库名称列表
        """
        success, results = self.cypher_executor.execute_transactional_cypher(GET_ALL_DATABASES)
        
        if not success:
            self.logger.error("获取所有数据库失败")
            return []
        
        databases = [result['database_name'] for result in results]
        self._log_info(f"找到 {len(databases)} 个数据库: {databases}")
        return databases

def main():
    """
    主函数 - 对所有数据库进行描述生成处理 (并发版本)
    """
    # 设置日志 - 只显示ERROR级别
    logging.basicConfig(
        level=logging.ERROR,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    print("=== 数据库描述生成系统 ===")
    
    # 初始化描述生成器
    print("初始化描述生成器...")
    desc_generator = GenerateDescription(enable_info_logging=False)  # 关闭调试日志
    
    try:
        # 验证数据库连接
        print("验证数据库连接...")
        if not desc_generator.cypher_executor.verify_connectivity():
            print("❌ 数据库连接失败")
            return
        print("✅ 数据库连接成功")
        
        # 获取所有数据库
        print("获取所有数据库...")
        databases = desc_generator.get_all_databases()
        
        if not databases:
            print("❌ 未找到任何数据库")
            return
        
        print(f"✅ 找到 {len(databases)} 个数据库")
        
        # 初始化总体统计
        total_stats = {
            "total_standalone_tables": 0,
            "described_standalone_tables": 0,
            "total_shared_groups": 0,
            "described_shared_groups": 0,
            "newly_described_tables": 0,
            "newly_described_groups": 0,
            "failed_databases": []
        }
        
        # 并发处理数据库
        with ThreadPoolExecutor(max_workers=5) as executor:
            # 创建future到数据库名的映射
            future_to_db = {
                executor.submit(desc_generator.generate_descriptions_for_database, db, False): db 
                for db in databases
            }
            
            # 使用进度条显示总体进度
            with tqdm(total=len(databases), desc="处理数据库") as pbar:
                for future in as_completed(future_to_db):
                    database_name = future_to_db[future]
                    try:
                        desc_success, desc_stats = future.result()
                        
                        # 更新总体统计
                        total_stats["total_standalone_tables"] += desc_stats.get("total_standalone_tables", 0)
                        total_stats["described_standalone_tables"] += desc_stats.get("described_standalone_tables", 0)
                        total_stats["total_shared_groups"] += desc_stats.get("total_shared_groups", 0)
                        total_stats["described_shared_groups"] += desc_stats.get("described_shared_groups", 0)
                        total_stats["newly_described_tables"] += desc_stats.get("newly_described_tables", 0)
                        total_stats["newly_described_groups"] += desc_stats.get("newly_described_groups", 0)
                        
                        if not desc_success:
                            total_stats["failed_databases"].append(database_name)
                            
                    except Exception as e:
                        logger.error(f"处理数据库 {database_name} 时出现异常: {e}")
                        total_stats["failed_databases"].append(f"{database_name}(异常: {str(e)})")
                    
                    finally:
                        pbar.update(1)
        
        # 输出最终统计
        print("\n" + "=" * 60)
        print("📊 描述生成完成总体统计:")
        print(f"\n1. 数据库概况:")
        print(f"  - 总数据库数: {len(databases)}")
        print(f"  - 成功处理: {len(databases) - len(total_stats['failed_databases'])}")
        print(f"  - 处理失败: {len(total_stats['failed_databases'])}")
        
        print(f"\n2. Standalone表统计:")
        print(f"  - 总表数: {total_stats['total_standalone_tables']}")
        print(f"  - 已有描述: {total_stats['described_standalone_tables']}")
        print(f"  - 新生成描述: {total_stats['newly_described_tables']}")
        
        total_tables = total_stats['total_standalone_tables']
        if total_tables > 0:
            coverage = ((total_stats['described_standalone_tables'] + total_stats['newly_described_tables']) / total_tables * 100)
            print(f"  - 覆盖率: {coverage:.1f}%")
        
        print(f"\n3. SharedFieldGroup统计:")
        print(f"  - 总组数: {total_stats['total_shared_groups']}")
        print(f"  - 已有描述: {total_stats['described_shared_groups']}")
        print(f"  - 新生成描述: {total_stats['newly_described_groups']}")
        
        total_groups = total_stats['total_shared_groups']
        if total_groups > 0:
            coverage = ((total_stats['described_shared_groups'] + total_stats['newly_described_groups']) / total_groups * 100)
            print(f"  - 覆盖率: {coverage:.1f}%")
        
        if total_stats["failed_databases"]:
            print("\n4. 失败的数据库:")
            for failed_db in total_stats["failed_databases"]:
                print(f"  - {failed_db}")
        
        # 计算总体成功率
        total_nodes = total_stats['total_standalone_tables'] + total_stats['total_shared_groups']
        if total_nodes > 0:
            total_described = (total_stats['described_standalone_tables'] + total_stats['newly_described_tables'] +
                             total_stats['described_shared_groups'] + total_stats['newly_described_groups'])
            coverage_rate = total_described / total_nodes
            print(f"\n总体描述覆盖率: {coverage_rate:.1%}")
            
            if coverage_rate >= 0.8:
                print("\n🎉 描述生成整体成功!")
            else:
                print("\n⚠️  描述覆盖率较低，请检查失败原因")
        
        print("\n💡 提示: 完成描述生成后，可以运行向量化程序进行下一步处理")
        
    except Exception as e:
        logger.error(f"主程序异常: {e}")
        print(f"❌ 处理过程中出现严重错误: {e}")
    
    finally:
        # 关闭连接
        desc_generator.close()
        print("=== 描述生成程序结束 ===")

if __name__ == "__main__":
    main()