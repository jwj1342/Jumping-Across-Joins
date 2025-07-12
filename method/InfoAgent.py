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
from langchain_core.tools import tool
from vectorization import VectorizedFieldManager
from method.prompts import FIELD_EXTRACTION_PROMPT, field_extraction_parser
from method.CypherTemplate import TABLE_BASED_DB_STRUCTURE_TREE_QUERY

# 设置日志 - 避免重复输出
_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)
# 防止日志向上传播到根logger，避免重复输出
_logger.propagate = False
if not _logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    _logger.addHandler(handler)


# ===== 核心函数式API =====

@tool
def search_related_fields(query: List[str], database_id: str, top_k: int = 3) -> List[Dict[str, Any]]:
    """
    在指定数据库中搜索与查询相关的字段
    
    Args:
        query: 查询字符串列表
        database_id: 数据库ID
        top_k: 返回的最大结果数量
    
    Returns:
        相关字段的列表，包含字段信息和相似度分数
    """
    if not query or not database_id:
        _logger.warning("查询列表或数据库ID为空")
        return []
    
    try:
        vector_manager = VectorizedFieldManager(enable_info_logging=False)
        all_results = []
        seen_field_ids = set()
        
        # 对每个查询进行搜索
        for query_text in query:
            if not query_text.strip():
                continue
                
            results = vector_manager.search_fields(query_text.strip(), database_id, top_k)
            
            # 添加未见过的结果
            for result in results:
                field_id = result.get('field_id')
                if field_id and field_id not in seen_field_ids:
                    seen_field_ids.add(field_id)
                    # 添加查询信息以便跟踪
                    result['matched_query'] = query_text.strip()
                    all_results.append(result)
        
        # 按相似度分数排序（降序）
        all_results.sort(key=lambda x: x.get('similarity_score', 0), reverse=True)
        
        # 返回前top_k个结果
        final_results = all_results[:top_k]
        
        _logger.info(f"为数据库 '{database_id}' 的 {len(query)} 个查询找到 {len(final_results)} 个相关字段")
        return final_results
        
    except Exception as e:
        _logger.error(f"搜索相关字段时出错: {e}")
        return []
    finally:
        # 确保资源被正确释放
        if 'vector_manager' in locals():
            vector_manager.close()


def get_intelligent_db_summary(database_id: str, user_query: str, top_k: int = 10) -> Dict[str, Any]:
    """
    基于用户查询智能生成数据库摘要树
    
    Args:
        database_id: 数据库ID
        user_query: 用户查询
        top_k: 搜索返回的最大字段数
        
    Returns:
        智能生成的数据库摘要字典
    """
    _logger.info(f"为数据库 '{database_id}' 基于用户查询生成智能摘要...")
    
    try:
        # 1. 使用LLM和提示模板提取潜在字段
        llm = initialize_llm()
        if not llm:
            _logger.error("LLM初始化失败，无法提取字段")
            return {}
            
        # 计算字段数量限制：基于top_k，但设置合理的上下限
        # 策略：允许比top_k稍多一些的字段提取，但不超过上限
        max_fields = min(max(top_k, 5), 20)  # 最少5个，最多20个
        _logger.info(f"基于top_k={top_k}，设置最大字段提取数量为{max_fields}")
            
        # 创建并执行chain
        chain = FIELD_EXTRACTION_PROMPT | llm | field_extraction_parser
        
        # 调用chain提取字段，传入max_fields参数
        response = chain.invoke({
            "user_query": user_query,
            "max_fields": max_fields
        })
        
        # 从字典中获取fields
        potential_fields = response.get('fields', [])
        
        # 双重保险：确保字段数量不超过限制
        if len(potential_fields) > max_fields:
            potential_fields = potential_fields[:max_fields]
            _logger.info(f"字段数量超过限制，截取前{max_fields}个字段")
        
        if not potential_fields:
            _logger.warning("未从查询中提取到潜在字段，使用整个查询进行搜索")
            potential_fields = [user_query]
        
        _logger.info(f"成功提取{len(potential_fields)}个潜在字段：{potential_fields}")
        
        # 2. 使用search_related_fields搜索相关字段
        related_fields = search_related_fields.invoke({
            "query": potential_fields,
            "database_id": database_id,
            "top_k": top_k
        })
        
        if not related_fields:
            _logger.error("未找到相关字段")
            return {}
        
        # 3. 提取字段ID和表名
        field_ids = [field['field_id'] for field in related_fields if field.get('field_id')]
        
        # 直接使用向量数据库中的全限定表名，因为图数据库中的表名也是全限定格式
        raw_table_names = [field['table'] for field in related_fields if field.get('table')]
        # 直接使用原始表名，不进行分割，因为图数据库中表名就是全限定格式(schema.table_name)
        table_names = list(set(raw_table_names))  # 去重
        
        _logger.info(f"步骤3: 提取到 {len(field_ids)} 个有效字段ID，来自 {len(table_names)} 个不同的表")
        _logger.info(f"全限定表名: {table_names}")
        
        if not table_names:
            _logger.error("未获取到有效表名")
            return {}
        
        # 4. 构建目标摘要树 - 基于相关表名的查询
        _logger.info(f"步骤4: 基于 {len(table_names)} 个相关表构建完整摘要树...")
        
        # 调试步骤：验证数据库和表的存在
        cypher_executor = CypherExecutor(enable_info_logging=True)
        
        try:
            # 直接使用表名查询完整字段信息
            success, graph_results = cypher_executor.execute_transactional_cypher(
                TABLE_BASED_DB_STRUCTURE_TREE_QUERY, 
                {
                    "database_id": database_id,
                    "table_names": table_names
                }
            )
            _logger.info(f"使用表名查询完整字段信息: {table_names}")
            
            if success and graph_results:
                summary = graph_results[0].get('dbSummary', {})
                _logger.info(f"成功构建包含 {len(table_names)} 个相关表的完整字段摘要树")
            else:
                _logger.error(f"使用表名查询失败，可能的原因：")
                _logger.error(f"  1. 数据库 '{database_id}' 未在图数据库中构建")
                _logger.error(f"  2. 全限定表名不存在：{table_names}")
                _logger.error(f"  建议：请先确保图数据库已正确构建并包含这些表")
                return {}
                
        except Exception as e:
            _logger.error(f"构建目标摘要树时出错: {e}")
            return {}
        
        if not summary:
            _logger.error("目标摘要构建失败: summary为空")
            return {}
        
        # 5. 添加搜索元信息
        summary['_search_metadata'] = {
            'user_query': user_query,
            'top_k': top_k,
            'max_fields': max_fields,
            'extracted_fields': potential_fields,
            'extracted_fields_count': len(potential_fields),
            'found_fields_count': len(related_fields),
            'related_tables': table_names,  # 全限定表名(schema.table_name)
            'table_count': len(table_names)
        }
        
        _logger.info(f"成功为数据库 '{database_id}' 生成包含相关表完整字段的智能摘要")
        return summary
        
    except Exception as e:
        _logger.error(f"智能摘要生成失败: {e}")
        return {}


if __name__ == "__main__":
    print("=== InfoAgent功能测试 ===\n")
    
    # 测试配置
    test_database = "CRYPTO"
    test_user_query = "Find all user information including addresses and transaction history"
    
    # 1. 测试search_related_fields函数
    print("1. 测试search_related_fields函数...")
    test_queries = ["user information", "transaction data", "block data"]
    print(f"数据库: {test_database}")
    print(f"查询词: {test_queries}")
    
    results = search_related_fields.invoke({
        "query": test_queries,
        "database_id": test_database,
        "top_k": 5
    })
    
    if results:
        print("\n找到以下相关字段:")
        for result in results:
            print(f"\n- 字段: {result['field_name']} ({result['field_type']})")
            print(f"  表: {result['table']}")
            print(f"  匹配查询: {result['matched_query']}")
            print(f"  相似度分数: {result['similarity_score']:.3f}")
    else:
        print("\n未找到相关字段")
    
    print("\n" + "="*50)
    
    # 2. 测试智能摘要生成（包含相关表的所有字段）
    print("\n2. 测试智能摘要生成（包含相关表的所有字段）...")
    print(f"数据库: {test_database}")
    print(f"用户查询: {test_user_query}")
    
    intelligent_summary = get_intelligent_db_summary(test_database, test_user_query)
    
    if intelligent_summary:
        print("\n✅ 智能摘要生成成功!")
        print(f"数据库: {intelligent_summary.get('database', 'N/A')}")
        
        # 显示搜索元信息
        metadata = intelligent_summary.get('_search_metadata', {})
        if metadata:
            print(f"\n搜索元信息:")
            print(f"  - 原始查询: {metadata.get('user_query', 'N/A')}")
            print(f"  - top_k参数: {metadata.get('top_k', 'N/A')}")
            print(f"  - 字段提取限制: {metadata.get('max_fields', 'N/A')}")
            print(f"  - 提取字段: {metadata.get('extracted_fields', [])}")
            print(f"  - 提取字段数: {metadata.get('extracted_fields_count', 0)}")
            print(f"  - 找到相关字段数: {metadata.get('found_fields_count', 0)}")
            print(f"  - 相关表数: {metadata.get('table_count', 0)}")
            print(f"  - 相关表名(全限定): {metadata.get('related_tables', [])}")
        
        # 显示摘要结构
        schemas = intelligent_summary.get('schemas', [])
        print(f"\n摘要结构:")
        for schema in schemas:
            schema_name = schema.get('schema', 'Unknown')
            tables = schema.get('tables', [])
            print(f"  Schema: {schema_name} ({len(tables)} 个表)")
            
            for table in tables[:3]:  # 只显示前3个表
                table_name = table.get('table', 'Unknown')
                fields = table.get('fields', [])
                print(f"    Table: {table_name} ({len(fields)} 个字段)")
                
                for field in fields[:3]:  # 只显示前3个字段
                    if isinstance(field, dict):
                        field_name = field.get('name', 'Unknown')
                        field_type = field.get('type', 'Unknown')
                        field_desc = field.get('description', '')
                        field_id = field.get('field_id', 'Unknown')
                        print(f"      - {field_name} ({field_type})")
                        if field_desc:
                            print(f"        描述: {field_desc}")
                        print(f"        字段ID: {field_id}")
                    else:
                        print(f"      - {field}")
    else:
        print("\n❌ 智能摘要生成失败")
    
    print("\n" + "="*50)
    
    
    print("\n=== 测试完成 ===")