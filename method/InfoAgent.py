"""
InfoAgent 的目标可以拆解为：
1. 面向用Query的问题相关性：不是全盘扫描所有结构，而是以Query为导向，收集相关表/字段的信息。
2. 结构摘要压缩（高信息熵）：把图结构压缩成SQLAgent可理解的自然语言描述或结构化摘要。
3. 错误驱动的结构补全：当SQLAgent失败时，自动对图结构进行局部拓展以获取缺失信息。
InfoAgent 可以实现的 API 接口
- get_all_tables()：返回全部表及字段
- get_table_fields(table_name)：返回指定表的字段
- find_tables_by_field(field_name)：字段反向查表
- summarize_related_schema(keywords: List[str])：根据query关键词生成相关schema描述
- suggest_similar_fields(field_name)：根据错误字段提示推荐相似字段及其所在表

其中需要注意的有两点
1. 信息压缩策略（高信息熵文本生成）
2. 错误反馈后的增量探索：SQLAgent在执行出错后，InfoAgent可以根据错误提示反向定位相关字段或表，并：动态拓展图结构探索范围（例如只初始探索部分schema，出错后拓展更多）以及补充字段来源：如出错信息是"column X not found"，则 InfoAgent 应查询图中是否有字段名相似的Field节点，并返回其所属表。
"""

import re
import json
import logging
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from difflib import SequenceMatcher

# 添加项目根目录到路径，以便导入utils模块
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.CypherExecutor import CypherExecutor
from utils.SnowConnect import snowflake_sql_query
from utils.sql_templates import *
from utils.init_llm import initialize_llm
from prompts import INFO_AGENT_PROMPT, SCHEMA_SUMMARY_PROMPT, ERROR_ANALYSIS_PROMPT
from Communicate import (
    SystemState, InfoRequest, InfoResponse, InteractionType,
    get_current_schema_summary
)


class InfoAgent:
    """
    数据库Schema信息探索Agent
    负责从图数据库和Snowflake数据库中探索和收集相关的表结构信息
    """
    
    def __init__(self, enable_logging: bool = False):
        """
        初始化InfoAgent
        
        Args:
            enable_logging: 是否启用日志
        """
        self.cypher_executor = CypherExecutor(enable_info_logging=enable_logging)
        self.llm = initialize_llm()
        self.logger = logging.getLogger(__name__)
        if enable_logging:
            logging.basicConfig(level=logging.INFO)
            
        # 缓存机制
        self.schema_cache = {}
        self.similarity_cache = {}
        
    def get_all_tables(self, database_id: str) -> Dict[str, Any]:
        """
        获取Neo4j数据库中的所有表信息
        
        Args:
            database_id: 数据库ID
            
        Returns:
            包含所有表信息的字典
        """
        try:
            # 使用Cypher查询获取所有表信息
            # 查找指定数据库下的所有Schema和Table
            cypher_query = f"""
            MATCH (d:Database {{name: '{database_id}'}})-[:HAS_SCHEMA]->(s:Schema)-[:HAS_TABLE]->(t:Table)
            RETURN s.name as schema_name, t.name as table_name, t.type as table_type, 
                   t.created as created, t.row_count as row_count
            """
            
            success, graph_results = self.cypher_executor.execute_transactional_cypher(cypher_query)
            
            if not success or not graph_results:
                self.logger.warning(f"未找到数据库 {database_id} 的表信息")
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
                    'row_count': result.get('row_count', 0)
                }
            
            return all_tables
            
        except Exception as e:
            self.logger.error(f"获取所有表信息失败: {e}")
            return {}
    
    def get_table_fields(self, table_name: str, database_id: str) -> Dict[str, Any]:
        """
        获取指定表的字段信息
        
        Args:
            table_name: 表名（可能包含schema）
            database_id: 数据库ID
            
        Returns:
            包含表字段信息的字典
        """
        try:
            # TODO：完善从neo4j中取出所有的filed的信息。
            # # 解析表名
            # if '.' in table_name:
            #     schema_name, table_name_only = table_name.split('.', 1)
            # else:
            #     # 如果没有指定schema，尝试从所有schema中查找
            #     schemas_result = snowflake_sql_query(GET_ALL_SCHEMAS, database_id)
            #     schema_name = None
            #     table_name_only = table_name
                
            #     for schema_row in schemas_result:
            #         test_schema = schema_row['SCHEMA_NAME']
            #         try:
            #             test_sql = GET_TABLES_BASIC.format(schema_name=test_schema)
            #             tables_in_schema = snowflake_sql_query(test_sql, database_id)
                        
            #             for table_row in tables_in_schema:
            #                 if table_row['TABLE_NAME'].upper() == table_name.upper():
            #                     schema_name = test_schema
            #                     break
                        
            #             if schema_name:
            #                 break
            #         except:
            #             continue
                
            #     if not schema_name:
            #         raise ValueError(f"未找到表 {table_name}")
            
            # # 获取字段信息
            # columns_sql = GET_COLUMNS_FOR_TABLE.format(
            #     schema_name=schema_name, 
            #     table_name=table_name_only
            # )
            # columns_result = snowflake_sql_query(columns_sql, database_id)
            
            # table_info = {
            #     'schema': schema_name,
            #     'table': table_name_only,
            #     'fields': []
            # }
            
            # for col_row in columns_result:
            #     field_info = {
            #         'name': col_row['COLUMN_NAME'],
            #         'type': col_row['DATA_TYPE'],
            #         'nullable': col_row['IS_NULLABLE'] == 'YES',
            #         'position': col_row['ORDINAL_POSITION'],
            #         'default': col_row.get('COLUMN_DEFAULT'),
            #         'max_length': col_row.get('CHARACTER_MAXIMUM_LENGTH'),
            #         'precision': col_row.get('NUMERIC_PRECISION'),
            #         'scale': col_row.get('NUMERIC_SCALE')
            #     }
            #     table_info['fields'].append(field_info)
            
            # 直接返回表信息，不需要写入图数据库
            return table_info
            
        except Exception as e:
            self.logger.error(f"获取表 {table_name} 字段信息失败: {e}")
            return {}
    
    def find_tables_by_field(self, field_name: str, database_id: str) -> List[Dict[str, Any]]:
        """
        通过字段名查找包含该字段的表
        
        Args:
            field_name: 字段名
            database_id: 数据库ID
            
        Returns:
            包含该字段的表列表
        """
        try:
            # 先尝试从图数据库查询（可能会失败如果关系不存在）
            matching_tables = []
            try:
                cypher_query = f"""
                MATCH (f:Field {{name: '{field_name}'}})-[:BELONGS_TO]->(t:Table)
                RETURN t.name as table_name, t.schema as schema_name, f.type as field_type
                """
                
                success, graph_results = self.cypher_executor.execute_transactional_cypher(cypher_query)
                
                if success and graph_results:
                    return [
                        {
                            'table': result['table_name'],
                            'schema': result['schema_name'],
                            'field_type': result['field_type']
                        }
                        for result in graph_results
                    ]
            except Exception as graph_e:
                self.logger.debug(f"图数据库查询失败，回退到Snowflake查询: {graph_e}")
            
            # 如果图数据库没有结果，从Snowflake直接查询
            schemas_result = snowflake_sql_query(GET_ALL_SCHEMAS, database_id)
            matching_tables = []
            
            for schema_row in schemas_result:
                schema_name = schema_row['SCHEMA_NAME']
                
                # 查询该schema下的所有表
                tables_sql = GET_TABLES_BASIC.format(schema_name=schema_name)
                tables_result = snowflake_sql_query(tables_sql, database_id)
                
                for table_row in tables_result:
                    table_name = table_row['TABLE_NAME']
                    
                    # 检查该表是否包含指定字段
                    try:
                        columns_sql = GET_COLUMNS_BASIC.format(
                            schema_name=schema_name,
                            table_name=table_name
                        )
                        columns_result = snowflake_sql_query(columns_sql, database_id)
                        
                        for col_row in columns_result:
                            if col_row['COLUMN_NAME'].upper() == field_name.upper():
                                matching_tables.append({
                                    'table': table_name,
                                    'schema': schema_name,
                                    'field_type': col_row['DATA_TYPE']
                                })
                                break
                    except:
                        continue
            
            return matching_tables
            
        except Exception as e:
            self.logger.error(f"通过字段 {field_name} 查找表失败: {e}")
            return []
    
    def suggest_similar_fields(self, field_name: str, database_id: str, threshold: float = 0.6) -> List[Dict[str, Any]]:
        """
        根据字段名推荐相似的字段
        
        Args:
            field_name: 目标字段名
            database_id: 数据库ID
            threshold: 相似度阈值
            
        Returns:
            相似字段列表
        """
        try:
            similar_fields = []
            target_field_lower = field_name.lower()
            
            # 尝试从图数据库获取所有字段
            try:
                cypher_query = """
                MATCH (f:Field)-[:BELONGS_TO]->(t:Table)
                RETURN f.name as field_name, t.name as table_name, t.schema as schema_name, f.type as field_type
                """
                
                success, graph_results = self.cypher_executor.execute_transactional_cypher(cypher_query)
                
                if success and graph_results:
                    for result in graph_results:
                        candidate_field = result['field_name'].lower()
                        similarity = SequenceMatcher(None, target_field_lower, candidate_field).ratio()
                        
                        if similarity >= threshold:
                            similar_fields.append({
                                'field_name': result['field_name'],
                                'table': result['table_name'],
                                'schema': result['schema_name'],
                                'field_type': result['field_type'],
                                'similarity': similarity
                            })
            except Exception as graph_e:
                self.logger.debug(f"图数据库查询失败，无法获取相似字段: {graph_e}")
                # 当图数据库查询失败时，返回空列表（可以在这里添加从Snowflake查询的逻辑）
            
            # 按相似度排序
            similar_fields.sort(key=lambda x: x['similarity'], reverse=True)
            
            return similar_fields[:10]  # 返回前10个最相似的
            
        except Exception as e:
            self.logger.error(f"查找相似字段失败: {e}")
            return []
    
    def summarize_related_schema(self, keywords: List[str], database_id: str) -> str:
        """
        根据关键词生成相关schema的摘要描述
        
        Args:
            keywords: 关键词列表
            database_id: 数据库ID
            
        Returns:
            Schema摘要文本
        """
        try:
            related_info = {}
            
            # 构建所有关键词的查询语句
            table_queries = []
            field_queries = []
            for keyword in keywords:
                # 查找表名包含关键词的表
                table_queries.append(f"""
                MATCH (t:Table)
                WHERE toLower(t.name) CONTAINS toLower('{keyword}') 
                   OR toLower(t.schema) CONTAINS toLower('{keyword}')
                RETURN t.name as table_name, t.schema as schema_name
                """)
                
                # 查找字段名包含关键词的字段
                field_queries.append(f"""
                MATCH (f:Field)-[:BELONGS_TO]->(t:Table)
                WHERE toLower(f.name) CONTAINS toLower('{keyword}')
                RETURN f.name as field_name, t.name as table_name, t.schema as schema_name
                """)
            
            # 合并所有查询语句并执行
            all_queries = ";\n".join(table_queries + field_queries)
            success, all_results = self.cypher_executor.execute_transactional_cypher(all_queries)
            
            if success and all_results:
                # 处理表查询结果
                for result in all_results[:len(keywords)]:  # 前半部分是表查询结果
                    table_key = f"{result['schema_name']}.{result['table_name']}"
                    if table_key not in related_info:
                        # 获取表的详细字段信息
                        table_details = self.get_table_fields(table_key, database_id)
                        related_info[table_key] = table_details
                
                # 处理字段查询结果
                for result in all_results[len(keywords):]:  # 后半部分是字段查询结果
                    table_key = f"{result['schema_name']}.{result['table_name']}"
                    if table_key not in related_info:
                        table_details = self.get_table_fields(table_key, database_id)
                        related_info[table_key] = table_details
            
            # 使用LLM生成摘要
            if related_info and self.llm:
                raw_data = json.dumps(related_info, indent=2, ensure_ascii=False)
                prompt = SCHEMA_SUMMARY_PROMPT.format(raw_data=raw_data)
                
                response = self.llm.invoke(prompt)
                return response.content if hasattr(response, 'content') else str(response)
            
            # 如果没有LLM，生成简单摘要
            summary_parts = []
            for table_key, table_info in related_info.items():
                if isinstance(table_info, dict) and 'fields' in table_info:
                    field_count = len(table_info['fields'])
                    key_fields = [f['name'] for f in table_info['fields'][:3]]
                    summary_parts.append(
                        f"表 {table_key}: {field_count}个字段，主要字段: {', '.join(key_fields)}"
                    )
            
            return "; ".join(summary_parts) if summary_parts else "未找到相关Schema信息"
            
        except Exception as e:
            self.logger.error(f"生成Schema摘要失败: {e}")
            return f"Schema摘要生成失败: {e}"
    
    def analyze_sql_error(self, sql_query: str, error_message: str, database_id: str) -> Dict[str, Any]:
        """
        分析SQL错误并提供修复建议
        
        Args:
            sql_query: 出错的SQL语句
            error_message: 错误信息
            database_id: 数据库ID
            
        Returns:
            错误分析结果
        """
        try:
            # 使用LLM分析错误
            if self.llm:
                prompt = ERROR_ANALYSIS_PROMPT.format(
                    sql_query=sql_query,
                    error_message=error_message,
                    database_id=database_id
                )
                
                response = self.llm.invoke(prompt)
                try:
                    # 尝试解析JSON响应
                    analysis_result = json.loads(response.content if hasattr(response, 'content') else str(response))
                except json.JSONDecodeError:
                    # 如果不是JSON，创建默认结构
                    analysis_result = {
                        "error_type": "unknown",
                        "cause": response.content if hasattr(response, 'content') else str(response),
                        "missing_info": [],
                        "suggestions": []
                    }
            else:
                # 基础错误分析
                analysis_result = self._basic_error_analysis(sql_query, error_message)
            
            # 补充相似名称建议
            if "not found" in error_message.lower() or "does not exist" in error_message.lower():
                # 提取错误中的表名或字段名
                missing_names = self._extract_missing_names(error_message)
                
                for missing_name in missing_names:
                    # 查找相似的表名或字段名
                    similar_suggestions = self.suggest_similar_fields(missing_name, database_id)
                    if similar_suggestions:
                        analysis_result["suggestions"].extend([
                            f"是否是 {sugg['field_name']} (表: {sugg['schema']}.{sugg['table']})?"
                            for sugg in similar_suggestions[:3]
                        ])
            
            return analysis_result
            
        except Exception as e:
            self.logger.error(f"分析SQL错误失败: {e}")
            return {
                "error_type": "analysis_failed",
                "cause": str(e),
                "missing_info": [],
                "suggestions": ["请检查SQL语法和表名/字段名是否正确"]
            }
    
    def process_info_request(self, state: SystemState, request: InfoRequest) -> InfoResponse:
        """
        处理来自SQLAgent的信息请求
        
        Args:
            state: 系统状态
            request: 信息请求
            
        Returns:
            信息响应
        """
        try:
            response_content = ""
            tables_info = {}
            relationships = []
            suggestions = []
            
            if request['message_type'] == InteractionType.INITIAL_SCHEMA:
                # 初始schema查询
                keywords = self._extract_keywords_from_query(state['user_query'])
                response_content = self.summarize_related_schema(keywords, state['database_id'])
                
            elif request['message_type'] == InteractionType.TABLE_FIELDS:
                # 特定表字段查询
                if request['specific_tables']:
                    for table_name in request['specific_tables']:
                        table_info = self.get_table_fields(table_name, state['database_id'])
                        if table_info:
                            tables_info[table_name] = table_info
                    response_content = f"已获取 {len(tables_info)} 个表的字段信息"
                
            elif request['message_type'] == InteractionType.ERROR_FEEDBACK:
                # 错误反馈处理
                if request['error_info']:
                    error_analysis = self.analyze_sql_error(
                        state.get('current_sql', ''),
                        request['error_info'],
                        state['database_id']
                    )
                    response_content = error_analysis['cause']
                    suggestions = error_analysis['suggestions']
                
            elif request['message_type'] == InteractionType.FIELD_MEANING:
                # 字段含义查询
                if request['specific_fields']:
                    for field_name in request['specific_fields']:
                        field_tables = self.find_tables_by_field(field_name, state['database_id'])
                        if field_tables:
                            tables_info[field_name] = field_tables
                    response_content = f"字段 {', '.join(request['specific_fields'])} 的相关信息"
            
            return {
                "message_type": request['message_type'],
                "content": response_content,
                "metadata": {},
                "timestamp": None,
                "tables_info": tables_info,
                "relationships": relationships,
                "suggestions": suggestions
            }
            
        except Exception as e:
            self.logger.error(f"处理信息请求失败: {e}")
            return {
                "message_type": request['message_type'],
                "content": f"处理请求失败: {e}",
                "metadata": {},
                "timestamp": None,
                "tables_info": {},
                "relationships": [],
                "suggestions": []
            }
    
    
    def _extract_keywords_from_query(self, query: str) -> List[str]:
        """从用户查询中提取关键词"""
        # 移除常见的停用词
        stop_words = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 
            'of', 'with', 'by', 'what', 'how', 'when', 'where', 'why', 'which',
            'is', 'are', 'was', 'were', 'be', 'been', 'have', 'has', 'had',
            'do', 'does', 'did', 'can', 'could', 'should', 'would', 'will'
        }
        
        # 简单的关键词提取
        words = re.findall(r'\b\w+\b', query.lower())
        keywords = [word for word in words if word not in stop_words and len(word) > 2]
        
        return keywords[:10]  # 返回前10个关键词
    
    def _basic_error_analysis(self, sql_query: str, error_message: str) -> Dict[str, Any]:
        """基础错误分析（不使用LLM）"""
        error_type = "unknown"
        cause = error_message
        suggestions = []
        
        error_lower = error_message.lower()
        
        if "table" in error_lower and ("not found" in error_lower or "does not exist" in error_lower):
            error_type = "table_not_found"
            suggestions.append("检查表名是否正确，可能需要包含schema名称")
            
        elif "column" in error_lower and ("not found" in error_lower or "does not exist" in error_lower):
            error_type = "column_not_found"
            suggestions.append("检查字段名是否正确，注意大小写")
            
        elif "syntax" in error_lower:
            error_type = "syntax_error"
            suggestions.append("检查SQL语法是否正确")
            
        return {
            "error_type": error_type,
            "cause": cause,
            "missing_info": [],
            "suggestions": suggestions
        }
    
    def _extract_missing_names(self, error_message: str) -> List[str]:
        """从错误信息中提取缺失的表名或字段名"""
        # 简单的正则表达式匹配
        patterns = [
            r"table '(\w+)'",
            r"column '(\w+)'",
            r"'(\w+)' not found",
            r"(\w+) does not exist"
        ]
        
        missing_names = []
        for pattern in patterns:
            matches = re.findall(pattern, error_message, re.IGNORECASE)
            missing_names.extend(matches)
        
        return list(set(missing_names))  # 去重