"""
SQLAgent是一个具备"规划、执行、自我反省"的智能体，它的目标是：基于InfoAgent给出的结构信息和用户query，构造准确有效的SQL，并确保执行成功。本质是一个交互式规划执行循环：
- 初始尝试生成SQL
- 若失败，分析错误 → 信息补全 → 重新生成
- 若成功但结果为空 → 判断语义问题 → 可能再修正
- 最终返回可解释的 SQL 与结果

有下面的几个能力：
1.  用户Query解析（自然语言理解）：将用户的自然语言转化为SQL的语义草图（如：涉及的实体、字段、筛选条件等）。
2.  表字段匹配与映射（依赖InfoAgent）：将Query中提取的关键词、字段、概念，与InfoAgent返回的结构信息对齐。
3. 将映射后的实体、字段组合为合法的SQL语句
4. 将SQL语句发送给数据库执行，返回执行结果或错误信息。反馈给 InfoAgent
5. 空结果检测

用户Query → Query解析器
         ↓
      SQL生成器 ← InfoAgent输出
         ↓
      SQL执行器 → 执行数据库 ←【错误反馈】→ 错误解释器 → InfoAgent
         ↓
 结果验证器（空检查、语义合理性）
         ↓
     最终SQL + 结果输出

"""

import json
import logging
import time
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import re

# 添加项目根目录到路径，以便导入utils模块
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.SnowConnect import snowflake_sql_query
from utils.init_llm import initialize_llm
from prompts import SQL_AGENT_PROMPT, ERROR_ANALYSIS_PROMPT
from Communicate import (
    SystemState, InfoRequest, InfoResponse, InteractionType, SQLExecutionResult
)


class SQLAgent:
    """
    SQL生成Agent
    负责根据用户查询和Schema信息生成准确的SQL语句，并处理执行错误
    """
    
    def __init__(self, enable_logging: bool = True):
        """
        初始化SQLAgent
        
        Args:
            enable_logging: 是否启用日志
        """
        self.llm = initialize_llm()
        self.logger = logging.getLogger(__name__)
        if enable_logging:
            logging.basicConfig(level=logging.INFO)
            
        # SQL生成历史
        self.sql_history = []
        self.error_patterns = {}
        
    def parse_user_query(self, user_query: str) -> Dict[str, Any]:
        """
        解析用户查询，提取关键信息
        
        Args:
            user_query: 用户的自然语言查询
            
        Returns:
            解析结果字典
        """
        try:
            # 提取查询类型
            query_type = self._identify_query_type(user_query)
            
            # 提取实体和字段
            entities = self._extract_entities(user_query)
            
            # 提取筛选条件
            filters = self._extract_filters(user_query)
            
            # 提取聚合操作
            aggregations = self._extract_aggregations(user_query)
            
            # 提取排序信息
            ordering = self._extract_ordering(user_query)
            
            # 提取时间范围
            time_range = self._extract_time_range(user_query)
            
            return {
                "query_type": query_type,
                "entities": entities,
                "filters": filters,
                "aggregations": aggregations,
                "ordering": ordering,
                "time_range": time_range,
                "complexity": self._assess_complexity(user_query)
            }
            
        except Exception as e:
            self.logger.error(f"解析用户查询失败: {e}")
            return {
                "query_type": "unknown",
                "entities": [],
                "filters": [],
                "aggregations": [],
                "ordering": None,
                "time_range": None,
                "complexity": "medium"
            }
    
    def generate_sql(self, state: SystemState, schema_info: Dict[str, Any]) -> str:
        """
        基于用户查询和Schema信息生成SQL
        
        Args:
            state: 系统状态
            schema_info: Schema信息
            
        Returns:
            生成的SQL语句
        """
        try:
            # 解析用户查询
            query_analysis = self.parse_user_query(state['user_query'])
            
            # 构建执行历史
            execution_history = self._build_execution_history(state)
            
            # 使用LLM生成SQL
            if self.llm:
                prompt = SQL_AGENT_PROMPT.format(
                    user_query=state['user_query'],
                    database_id=state['database_id'],
                    schema_info=json.dumps(schema_info, indent=2, ensure_ascii=False),
                    execution_history=execution_history
                )
                
                response = self.llm.invoke(prompt)
                response_text = response.content if hasattr(response, 'content') else str(response)
                
                # 尝试解析JSON响应
                try:
                    sql_response = json.loads(response_text)
                    sql_query = sql_response.get('sql_query', '')
                    self.logger.info(f"LLM生成SQL: {sql_query}")
                    
                    # 验证SQL基本格式
                    if self._validate_sql_syntax(sql_query):
                        return sql_query
                    else:
                        self.logger.warning("生成的SQL语法验证失败，尝试提取SQL")
                        return self._extract_sql_from_response(response_text)
                        
                except json.JSONDecodeError:
                    # 如果不是JSON格式，尝试提取SQL
                    return self._extract_sql_from_response(response_text)
            
            # 如果没有LLM，使用基础规则生成SQL
            return self._generate_basic_sql(query_analysis, schema_info)
            
        except Exception as e:
            self.logger.error(f"生成SQL失败: {e}")
            return f"-- SQL生成失败: {e}"
    
    def execute_sql(self, sql_query: str, database_id: str) -> SQLExecutionResult:
        """
        执行SQL查询
        
        Args:
            sql_query: SQL语句
            database_id: 数据库ID
            
        Returns:
            SQL执行结果
        """
        start_time = time.time()
        
        try:
            # 清理SQL语句
            cleaned_sql = self._clean_sql(sql_query)
            
            # 执行查询
            result_data = snowflake_sql_query(cleaned_sql, database_id)
            execution_time = time.time() - start_time
            
            # 记录成功执行
            self.sql_history.append({
                'sql': cleaned_sql,
                'success': True,
                'execution_time': execution_time,
                'result_count': len(result_data) if result_data else 0
            })
            
            self.logger.info(f"SQL执行成功，返回 {len(result_data) if result_data else 0} 行数据")
            
            return {
                "success": True,
                "sql_query": cleaned_sql,
                "result_data": result_data,
                "error_message": None,
                "execution_time": execution_time
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            error_message = str(e)
            
            # 记录执行失败
            self.sql_history.append({
                'sql': sql_query,
                'success': False,
                'error': error_message,
                'execution_time': execution_time
            })
            
            self.logger.error(f"SQL执行失败: {error_message}")
            
            return {
                "success": False,
                "sql_query": sql_query,
                "result_data": None,
                "error_message": error_message,
                "execution_time": execution_time
            }
    
    def analyze_execution_result(self, result: SQLExecutionResult, user_query: str) -> Dict[str, Any]:
        """
        分析SQL执行结果
        
        Args:
            result: SQL执行结果
            user_query: 用户查询
            
        Returns:
            分析结果
        """
        try:
            analysis = {
                "is_successful": result.success,
                "needs_retry": False,
                "semantic_validation": "unknown",
                "suggestions": [],
                "next_action": "complete"
            }
            
            if not result.success:
                # 执行失败分析
                analysis["needs_retry"] = True
                analysis["next_action"] = "request_info"
                analysis["error_analysis"] = self._analyze_error_type(result.error_message)
                analysis["suggestions"] = self._generate_error_suggestions(result.error_message)
                
            elif not result.result_data or len(result.result_data) == 0:
                # 结果为空分析
                analysis["semantic_validation"] = "empty_result"
                analysis["suggestions"] = [
                    "查询结果为空，可能需要检查筛选条件",
                    "确认表中是否有符合条件的数据",
                    "考虑放宽查询条件"
                ]
                
                # 判断是否需要重试
                if self._should_retry_for_empty_result(user_query, result):
                    analysis["needs_retry"] = True
                    analysis["next_action"] = "request_info"
                else:
                    analysis["next_action"] = "complete"
                    
            else:
                # 成功且有数据
                analysis["semantic_validation"] = "valid"
                analysis["result_summary"] = {
                    "row_count": len(result.result_data),
                    "columns": list(result.result_data[0].keys()) if result.result_data else [],
                    "sample_data": result.result_data[:3] if result.result_data else []
                }
                analysis["next_action"] = "complete"
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"分析执行结果失败: {e}")
            return {
                "is_successful": False,
                "needs_retry": True,
                "semantic_validation": "analysis_failed",
                "suggestions": ["结果分析失败，建议重新生成SQL"],
                "next_action": "request_info"
            }
    
    def create_info_request_from_error(self, error_message: str, sql_query: str) -> InfoRequest:
        """
        根据错误信息创建InfoAgent请求
        
        Args:
            error_message: 错误信息
            sql_query: 出错的SQL语句
            
        Returns:
            InfoAgent请求
        """
        try:
            # 分析错误类型
            error_analysis = self._analyze_error_type(error_message)
            
            if error_analysis["type"] == "table_not_found":
                return {
                    "message_type": InteractionType.ERROR_FEEDBACK,
                    "content": f"表不存在错误: {error_message}",
                    "metadata": {},
                    "timestamp": None,
                    "query_context": "",
                    "error_info": error_message,
                    "specific_tables": error_analysis.get("missing_tables", []),
                    "specific_fields": []
                }
                
            elif error_analysis["type"] == "column_not_found":
                return {
                    "message_type": InteractionType.FIELD_MEANING,
                    "content": f"字段不存在错误: {error_message}",
                    "metadata": {},
                    "timestamp": None,
                    "query_context": "",
                    "error_info": error_message,
                    "specific_tables": [],
                    "specific_fields": error_analysis.get("missing_fields", [])
                }
                
            elif error_analysis["type"] == "syntax_error":
                return {
                    "message_type": InteractionType.ERROR_FEEDBACK,
                    "content": f"SQL语法错误: {error_message}",
                    "metadata": {},
                    "timestamp": None,
                    "query_context": "",
                    "error_info": error_message,
                    "specific_tables": [],
                    "specific_fields": []
                }
                
            else:
                return {
                    "message_type": InteractionType.ERROR_FEEDBACK,
                    "content": f"SQL执行错误: {error_message}",
                    "metadata": {},
                    "timestamp": None,
                    "query_context": "",
                    "error_info": error_message,
                    "specific_tables": [],
                    "specific_fields": []
                }
                
        except Exception as e:
            self.logger.error(f"创建InfoAgent请求失败: {e}")
            return {
                "message_type": InteractionType.ERROR_FEEDBACK,
                "content": f"处理错误失败: {e}",
                "metadata": {},
                "timestamp": None,
                "query_context": "",
                "error_info": error_message,
                "specific_tables": [],
                "specific_fields": []
            }
    
    def validate_sql_quality(self, sql_query: str, user_query: str) -> Dict[str, Any]:
        """
        验证SQL质量
        
        Args:
            sql_query: SQL语句
            user_query: 用户查询
            
        Returns:
            质量评估结果
        """
        try:
            quality_score = 0.0
            issues = []
            suggestions = []
            
            # 基本语法检查
            if self._validate_sql_syntax(sql_query):
                quality_score += 0.3
            else:
                issues.append("SQL语法可能有问题")
                
            # 检查SQL复杂度
            complexity = self._assess_sql_complexity(sql_query)
            if complexity == "appropriate":
                quality_score += 0.2
            elif complexity == "too_complex":
                issues.append("SQL过于复杂，可能影响性能")
                suggestions.append("考虑简化查询或分步执行")
            
            # 检查表名和字段名格式
            if self._check_naming_conventions(sql_query):
                quality_score += 0.2
            else:
                issues.append("表名或字段名格式可能不符合规范")
                suggestions.append("确认表名和字段名是否正确")
            
            # 检查查询意图匹配
            intent_match = self._check_intent_match(sql_query, user_query)
            quality_score += intent_match * 0.3
            
            if intent_match < 0.5:
                issues.append("SQL可能不符合用户意图")
                suggestions.append("重新检查查询逻辑")
            
            return {
                "quality_score": quality_score,
                "issues": issues,
                "suggestions": suggestions,
                "is_acceptable": quality_score >= 0.7
            }
            
        except Exception as e:
            self.logger.error(f"验证SQL质量失败: {e}")
            return {
                "quality_score": 0.0,
                "issues": [f"质量检查失败: {e}"],
                "suggestions": ["建议重新生成SQL"],
                "is_acceptable": False
            }
    
    def _identify_query_type(self, query: str) -> str:
        """识别查询类型"""
        query_lower = query.lower()
        
        if any(word in query_lower for word in ['select', 'show', 'display', 'get', 'find', 'what']):
            return "select"
        elif any(word in query_lower for word in ['count', 'how many', 'number of']):
            return "count"
        elif any(word in query_lower for word in ['sum', 'total', 'average', 'avg', 'max', 'min']):
            return "aggregate"
        elif any(word in query_lower for word in ['group by', 'group', 'breakdown']):
            return "group"
        elif any(word in query_lower for word in ['join', 'combine', 'merge']):
            return "join"
        else:
            return "select"
    
    def _extract_entities(self, query: str) -> List[str]:
        """提取查询中的实体"""
        # 简单的实体提取，寻找可能的表名或实体名
        entities = []
        
        # 寻找引号中的内容
        quoted_entities = re.findall(r'"([^"]*)"', query)
        entities.extend(quoted_entities)
        
        # 寻找可能的表名（大写字母开头的单词）
        potential_tables = re.findall(r'\b[A-Z][a-z]+\b', query)
        entities.extend(potential_tables)
        
        return list(set(entities))
    
    def _extract_filters(self, query: str) -> List[Dict[str, str]]:
        """提取筛选条件"""
        filters = []
        
        # 寻找时间相关的筛选
        time_patterns = [
            r'in (\d{4})',  # 年份
            r'in (\d{4}-\d{2})',  # 年月
            r'(\d{4}-\d{2}-\d{2})',  # 日期
        ]
        
        for pattern in time_patterns:
            matches = re.findall(pattern, query)
            for match in matches:
                filters.append({
                    'type': 'time',
                    'value': match,
                    'condition': 'equals'
                })
        
        return filters
    
    def _extract_aggregations(self, query: str) -> List[str]:
        """提取聚合操作"""
        aggregations = []
        query_lower = query.lower()
        
        agg_keywords = {
            'sum': ['sum', 'total'],
            'count': ['count', 'number of', 'how many'],
            'avg': ['average', 'avg', 'mean'],
            'max': ['maximum', 'max', 'highest', 'largest'],
            'min': ['minimum', 'min', 'lowest', 'smallest']
        }
        
        for agg_type, keywords in agg_keywords.items():
            if any(keyword in query_lower for keyword in keywords):
                aggregations.append(agg_type)
        
        return aggregations
    
    def _extract_ordering(self, query: str) -> Optional[Dict[str, str]]:
        """提取排序信息"""
        query_lower = query.lower()
        
        if 'order by' in query_lower or 'sort' in query_lower:
            if 'desc' in query_lower or 'descending' in query_lower:
                return {'direction': 'DESC'}
            else:
                return {'direction': 'ASC'}
        
        return None
    
    def _extract_time_range(self, query: str) -> Optional[Dict[str, str]]:
        """提取时间范围"""
        # 查找年份
        year_match = re.search(r'\b(20\d{2})\b', query)
        if year_match:
            return {
                'type': 'year',
                'value': year_match.group(1)
            }
        
        # 查找日期范围
        date_match = re.search(r'\b(\d{4}-\d{2}-\d{2})\b', query)
        if date_match:
            return {
                'type': 'date',
                'value': date_match.group(1)
            }
        
        return None
    
    def _assess_complexity(self, query: str) -> str:
        """评估查询复杂度"""
        complexity_indicators = [
            'join', 'subquery', 'union', 'group by', 'having',
            'window function', 'case when', 'exists', 'not exists'
        ]
        
        query_lower = query.lower()
        complexity_count = sum(1 for indicator in complexity_indicators if indicator in query_lower)
        
        if complexity_count >= 3:
            return "high"
        elif complexity_count >= 1:
            return "medium"
        else:
            return "low"
    
    def _build_execution_history(self, state: SystemState) -> str:
        """构建执行历史摘要"""
        if not state.get('sql_execution_results'):
            return "无执行历史"
        
        history_parts = []
        for i, result in enumerate(state['sql_execution_results'][-3:], 1):  # 只显示最近3次
            if result.success:
                history_parts.append(f"{i}. 成功执行，返回{len(result.result_data or [])}行数据")
            else:
                history_parts.append(f"{i}. 执行失败: {result.error_message}")
        
        return "\n".join(history_parts)
    
    def _validate_sql_syntax(self, sql: str) -> bool:
        """基本SQL语法验证"""
        if not sql or not sql.strip():
            return False
            
        sql_lower = sql.lower().strip()
        
        # 检查是否以SQL关键字开头
        valid_starts = ['select', 'with', 'show', 'describe', 'explain']
        if not any(sql_lower.startswith(start) for start in valid_starts):
            return False
        
        # 检查基本的SQL结构
        if sql_lower.startswith('select'):
            return 'from' in sql_lower or 'dual' in sql_lower
        
        return True
    
    def _extract_sql_from_response(self, response_text: str) -> str:
        """从响应文本中提取SQL语句"""
        # 寻找代码块中的SQL
        code_block_pattern = r'```(?:sql)?\s*(.*?)\s*```'
        code_matches = re.findall(code_block_pattern, response_text, re.DOTALL | re.IGNORECASE)
        
        if code_matches:
            return code_matches[0].strip()
        
        # 寻找以SELECT开头的行
        lines = response_text.split('\n')
        for line in lines:
            line = line.strip()
            if line.upper().startswith('SELECT'):
                # 尝试获取完整的SQL语句
                sql_lines = [line]
                for next_line in lines[lines.index(line) + 1:]:
                    next_line = next_line.strip()
                    if not next_line or next_line.startswith('--'):
                        break
                    sql_lines.append(next_line)
                    if next_line.endswith(';'):
                        break
                
                return '\n'.join(sql_lines)
        
        return response_text.strip()
    
    def _generate_basic_sql(self, query_analysis: Dict[str, Any], schema_info: Dict[str, Any]) -> str:
        """生成基础SQL（不使用LLM）"""
        # 这是一个简化的SQL生成器
        if not schema_info:
            return "-- 无Schema信息，无法生成SQL"
        
        # 获取第一个可用的表
        first_table = None
        for table_name, table_data in schema_info.items():
            if isinstance(table_data, dict) and 'fields' in table_data:
                first_table = table_name
                break
        
        if not first_table:
            return "-- 无可用表信息"
        
        # 生成简单的SELECT语句
        table_data = schema_info[first_table]
        fields = table_data.get('fields', [])
        
        if not fields:
            return f"SELECT * FROM {first_table} LIMIT 10;"
        
        # 选择前几个字段
        selected_fields = [field['name'] for field in fields[:5]]
        fields_str = ', '.join(selected_fields)
        
        sql = f"SELECT {fields_str} FROM {first_table}"
        
        # 添加限制
        sql += " LIMIT 10;"
        
        return sql
    
    def _clean_sql(self, sql: str) -> str:
        """清理SQL语句"""
        # 移除注释
        sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
        
        # 移除多余的空白字符
        sql = re.sub(r'\s+', ' ', sql)
        sql = sql.strip()
        
        # 确保以分号结尾
        if not sql.endswith(';'):
            sql += ';'
        
        return sql
    
    def _analyze_error_type(self, error_message: str) -> Dict[str, Any]:
        """分析错误类型"""
        error_lower = error_message.lower()
        
        if "table" in error_lower and ("not found" in error_lower or "does not exist" in error_lower):
            # 提取表名
            table_matches = re.findall(r"table['\s]*(['\"]?)(\w+)\1", error_message, re.IGNORECASE)
            missing_tables = [match[1] for match in table_matches]
            
            return {
                "type": "table_not_found",
                "missing_tables": missing_tables,
                "message": error_message
            }
            
        elif "column" in error_lower and ("not found" in error_lower or "does not exist" in error_lower):
            # 提取字段名
            column_matches = re.findall(r"column['\s]*(['\"]?)(\w+)\1", error_message, re.IGNORECASE)
            missing_fields = [match[1] for match in column_matches]
            
            return {
                "type": "column_not_found",
                "missing_fields": missing_fields,
                "message": error_message
            }
            
        elif "syntax" in error_lower:
            return {
                "type": "syntax_error",
                "message": error_message
            }
            
        else:
            return {
                "type": "unknown",
                "message": error_message
            }
    
    def _generate_error_suggestions(self, error_message: str) -> List[str]:
        """生成错误修复建议"""
        suggestions = []
        error_analysis = self._analyze_error_type(error_message)
        
        if error_analysis["type"] == "table_not_found":
            suggestions.extend([
                "检查表名是否正确",
                "确认是否需要包含schema名称",
                "验证表是否存在于指定数据库中"
            ])
            
        elif error_analysis["type"] == "column_not_found":
            suggestions.extend([
                "检查字段名是否正确",
                "注意字段名的大小写",
                "确认字段是否存在于指定表中"
            ])
            
        elif error_analysis["type"] == "syntax_error":
            suggestions.extend([
                "检查SQL语法是否正确",
                "确认括号是否匹配",
                "检查关键字拼写"
            ])
            
        else:
            suggestions.append("请检查SQL语句的正确性")
        
        return suggestions
    
    def _should_retry_for_empty_result(self, user_query: str, result: SQLExecutionResult) -> bool:
        """判断空结果是否需要重试"""
        # 如果用户查询明确要求特定条件，空结果可能是正常的
        query_lower = user_query.lower()
        
        # 如果查询包含特定的筛选条件，空结果可能是正常的
        specific_conditions = [
            'where', 'specific', 'exact', 'particular',
            'only', 'just', 'exactly'
        ]
        
        if any(condition in query_lower for condition in specific_conditions):
            return False
        
        # 如果是计数查询，0结果可能是正常的
        if any(word in query_lower for word in ['count', 'how many', 'number of']):
            return False
        
        # 其他情况可能需要重试
        return True
    
    def _assess_sql_complexity(self, sql: str) -> str:
        """评估SQL复杂度"""
        sql_lower = sql.lower()
        
        complex_features = [
            'join', 'union', 'subquery', 'with', 'window',
            'partition by', 'row_number', 'rank', 'dense_rank',
            'case when', 'exists', 'not exists'
        ]
        
        complexity_count = sum(1 for feature in complex_features if feature in sql_lower)
        
        if complexity_count >= 4:
            return "too_complex"
        elif complexity_count >= 2:
            return "moderate"
        else:
            return "appropriate"
    
    def _check_naming_conventions(self, sql: str) -> bool:
        """检查命名规范"""
        # 简单检查：是否包含明显的SQL关键字
        sql_keywords = ['SELECT', 'FROM', 'WHERE', 'ORDER BY', 'GROUP BY']
        sql_upper = sql.upper()
        
        return any(keyword in sql_upper for keyword in sql_keywords)
    
    def _check_intent_match(self, sql: str, user_query: str) -> float:
        """检查SQL是否匹配用户意图"""
        # 简化的意图匹配检查
        sql_lower = sql.lower()
        query_lower = user_query.lower()
        
        match_score = 0.0
        
        # 检查聚合函数匹配
        if 'count' in query_lower and 'count' in sql_lower:
            match_score += 0.3
        if 'sum' in query_lower and 'sum' in sql_lower:
            match_score += 0.3
        if 'average' in query_lower and 'avg' in sql_lower:
            match_score += 0.3
        
        # 检查时间相关匹配
        if any(word in query_lower for word in ['2023', '2024']) and any(word in sql_lower for word in ['2023', '2024']):
            match_score += 0.2
        
        # 基础匹配（包含SELECT）
        if 'select' in sql_lower:
            match_score += 0.2
        
        return min(match_score, 1.0)