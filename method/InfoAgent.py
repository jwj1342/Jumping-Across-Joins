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
from vectorizaion_v2 import Vectorization
from method.prompts import FIELD_EXTRACTION_PROMPT, field_extraction_parser, NODE_EXTRACTION_PROMPT
from method.CypherTemplate import TABLE_BASED_DB_STRUCTURE_TREE_QUERY

# 设置日志 - 使用全局配置
_logger = logging.getLogger(__name__)
# 防止日志向上传播到根logger，避免重复输出
_logger.propagate = False
if not _logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    _logger.addHandler(handler)
    _logger.setLevel(logging.ERROR)  # 设置为ERROR级别


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


@tool
def search_related_node(
    query: List[str],
    database_id: str,
    top_k: int = 3
) -> List[Dict[str, Any]]:
    """
    在指定数据库中搜索与查询相关的节点（表和字段组）
    
    Args:
        query: 查询字符串列表
        database_id: 数据库ID
        top_k: 返回的最大结果数量
    
    Returns:
        相关节点的列表，包含节点信息和相似度分数
    """
    if not query or not database_id:
        _logger.warning("查询列表或数据库ID为空")
        return []
    
    try:
        vectorizer = Vectorization(enable_info_logging=False)
        all_results = []
        seen_element_ids = set()
        
        # 对每个查询进行搜索
        for query_text in query:
            if not query_text.strip():
                continue
                
            # 使用search_items方法搜索
            results = vectorizer.search_items(
                query=query_text.strip(),
                database_name=database_id,
                top_k=top_k
            )
            
            # 添加未见过的结果
            for result in results:
                element_id = result.get('element_id')
                if element_id and element_id not in seen_element_ids:
                    seen_element_ids.add(element_id)
                    # 添加查询信息以便跟踪
                    result['matched_query'] = query_text.strip()
                    all_results.append(result)
        
        # 按相似度分数排序（降序）
        all_results.sort(key=lambda x: x.get('similarity_score', 0), reverse=True)
        
        # 返回前top_k个结果
        final_results = all_results[:top_k]
        
        _logger.info(f"为数据库 '{database_id}' 的 {len(query)} 个查询找到 {len(final_results)} 个相关节点")
        return final_results
        
    except Exception as e:
        _logger.error(f"搜索相关节点时出错: {e}")
        return []
    finally:
        # 确保资源被正确释放
        if 'vectorizer' in locals():
            vectorizer.close()


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
        _logger.debug(f"基于top_k={top_k}，设置最大字段提取数量为{max_fields}")
            
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

def get_intelligent_db_summary_v2(database_id: str, user_query: str, top_k: int = 10) -> Dict[str, Any]:
    """
    基于用户查询智能生成数据库摘要树新版
    
    Args:
        database_id: 数据库ID
        user_query: 用户查询
        top_k: 搜索返回的最大节点数
        
    Returns:
        智能生成的数据库摘要字典
    """
    _logger.info(f"为数据库 '{database_id}' 基于用户查询生成智能摘要V2...")
    
    try:
        # 1. 使用LLM和提示模板提取潜在搜索词
        llm = initialize_llm()
        if not llm:
            _logger.error("LLM初始化失败，无法提取搜索词")
            return {}
            
        # 计算搜索词数量限制
        max_terms = min(max(top_k, 5), 20)  # 最少5个，最多20个
        
        # 使用新的NODE_EXTRACTION_PROMPT
        from method.prompts import NODE_EXTRACTION_PROMPT, field_extraction_parser
        
        # 创建并执行chain提取业务概念和主题
        chain = NODE_EXTRACTION_PROMPT | llm | field_extraction_parser
        response = chain.invoke({
            "user_query": user_query,
            "max_fields": max_terms
        })
        
        search_terms = response.get('fields', [])
        if len(search_terms) > max_terms:
            search_terms = search_terms[:max_terms]
        
        if not search_terms:
            _logger.warning("未从查询中提取到搜索词，使用整个查询进行搜索")
            search_terms = [user_query]
        
        _logger.info(f"成功提取{len(search_terms)}个搜索词用于节点搜索：{search_terms}")
        
        # 2. 使用search_related_node搜索相关节点
        related_nodes = search_related_node.invoke({
            "query": search_terms,
            "database_id": database_id,
            "top_k": top_k
        })
        
        if not related_nodes:
            _logger.error("未找到相关节点")
            return {}
            
        # 3. 初始化Neo4j执行器
        cypher_executor = CypherExecutor(enable_info_logging=True)
        
        # 4. 分类处理不同类型的节点
        tables_to_process = []  # 存储需要处理的表名
        
        for node in related_nodes:
            node_type = node.get('item_type')
            
            if node_type == 'table':
                # 直接添加表
                if node.get('name'):
                    tables_to_process.append(node['name'])
                    
            elif node_type == 'group_node':
                # 查询使用该group node的表
                group_node_id = node.get('element_id')
                if not group_node_id:
                    continue
                    
                # 查询使用该group node的表
                success, results = cypher_executor.execute_transactional_cypher(
                    """
                    MATCH (t:Table)-[:USES_FIELD_GROUP]->(g:FieldGroup)
                    WHERE g.id = $group_node_id
                    RETURN t.name as table_name
                    """,
                    {"group_node_id": group_node_id}
                )
                
                if success and results:
                    # 获取相关表名列表
                    related_tables = [r['table_name'] for r in results if r.get('table_name')]
                    
                    if related_tables:
                        # 使用LLM选择最相关的表（最多2-3个）
                        chain = (
                            "Given the user query: {query}\n"
                            "And these tables that use a related field group: {tables}\n"
                            "Select the 1-2 most relevant tables that would best answer the query.\n"
                            "Return only the table names, separated by commas."
                        ) | llm
                        
                        selected_tables_str = chain.invoke({
                            "query": user_query,
                            "tables": ", ".join(related_tables)
                        })
                        
                        # 处理LLM返回的表名
                        selected_tables = [t.strip() for t in selected_tables_str.split(',')]
                        tables_to_process.extend(selected_tables)
        
        # 去重
        tables_to_process = list(set(tables_to_process))
        
        if not tables_to_process:
            _logger.error("未找到需要处理的相关表")
            return {}
            
        # 5. 构建最终的摘要树
        success, graph_results = cypher_executor.execute_transactional_cypher(
            TABLE_BASED_DB_STRUCTURE_TREE_QUERY,
            {
                "database_id": database_id,
                "table_names": tables_to_process
            }
        )
        
        if success and graph_results:
            summary = graph_results[0].get('dbSummary', {})
            
            # 6. 添加搜索元信息
            summary['_search_metadata'] = {
                'user_query': user_query,
                'top_k': top_k,
                'max_terms': max_terms,
                'extracted_terms': search_terms,
                'extracted_terms_count': len(search_terms),
                'found_nodes_count': len(related_nodes),
                'processed_tables': tables_to_process,
                'table_count': len(tables_to_process),
                'version': 'v2'  # 标记这是v2版本的摘要
            }
            
            _logger.info(f"成功为数据库 '{database_id}' 生成V2版本的智能摘要")
            return summary
            
        else:
            _logger.error("构建摘要树失败")
            return {}
            
    except Exception as e:
        _logger.error(f"V2智能摘要生成失败: {e}")
        return {}

if __name__ == "__main__":
    # 测试新的 search_related_node 工具
    print("\n\n开始测试 search_related_node...")
    test_data_node = {
        'query': ['zip code boundaries', 'geographic data', 'employment data', 'time series'],
        'database_id': 'BLS',
        'top_k': 5
    }
    
    node_results = search_related_node.invoke({
        "query": test_data_node['query'],
        "database_id": test_data_node['database_id'],
        "top_k": test_data_node['top_k']
    })
    
    print(f"\n找到 {len(node_results)} 个相关节点:")
    for idx, result in enumerate(node_results, 1):
        print(f"\n结果 {idx}:")
        print(f"  节点类型: {result.get('item_type')}")
        print(f"  名称: {result.get('name')}")
        if result.get('schema'):
            print(f"  模式: {result.get('schema')}")
        print(f"  相似度: {result.get('similarity_score', 0):.4f}")
        print(f"  匹配查询: {result.get('matched_query')}")
        print(f"  描述摘要: {result.get('description')[:150]}...")