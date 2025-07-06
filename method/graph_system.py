"""
基于LangGraph的双Agent系统
整合InfoAgent和SQLAgent，实现自动化的SQL生成与错误修复流程
"""

import logging
from typing import Dict, Any, Literal, List, Union
from typing_extensions import TypedDict, Annotated
from operator import add

from langgraph.graph import StateGraph, END, START
from langgraph.graph.message import add_messages
from langgraph.types import Command
from langgraph.checkpoint.memory import MemorySaver

from InfoAgent import InfoAgent
from SqlAgent import SQLAgent
from Communicate import (
    SystemState, InteractionType, GraphSystemState,
    replace_reducer, list_append_reducer, dict_merge_reducer, increment_reducer,
    should_continue_iteration, get_current_schema_summary
)


class SQLGenerationSystem:
    """
    基于LangGraph的SQL生成系统
    协调InfoAgent和SQLAgent完成SQL生成与执行
    """
    
    def __init__(self, enable_logging: bool = True):
        """
        初始化系统
        
        Args:
            enable_logging: 是否启用日志
        """
        self.logger = logging.getLogger(__name__)
        if enable_logging:
            logging.basicConfig(level=logging.INFO)
            
        # 初始化Agents
        self.info_agent = InfoAgent(enable_logging)
        self.sql_agent = SQLAgent(enable_logging)
        
        # 创建LangGraph
        self.graph = self._create_graph()
        
    def _create_graph(self) -> StateGraph:
        """创建LangGraph工作流"""
        
        # 定义图
        workflow = StateGraph(GraphSystemState)
        
        # 添加节点
        workflow.add_node("initialize", self._initialize_node)
        workflow.add_node("info_exploration", self._info_exploration_node)
        workflow.add_node("sql_generation", self._sql_generation_node)
        workflow.add_node("sql_execution", self._sql_execution_node)
        workflow.add_node("error_analysis", self._error_analysis_node)
        workflow.add_node("result_validation", self._result_validation_node)
        workflow.add_node("finalize", self._finalize_node)
        
        # 设置入口点
        workflow.add_edge(START, "initialize")
        
        # 定义条件边
        workflow.add_conditional_edges(
            "initialize",
            self._route_after_initialize,
            {
                "info_exploration": "info_exploration",
                "error": "finalize"
            }
        )
        
        workflow.add_conditional_edges(
            "info_exploration", 
            self._route_after_info_exploration,
            {
                "sql_generation": "sql_generation",
                "continue_exploration": "info_exploration", 
                "error": "finalize"
            }
        )
        
        workflow.add_conditional_edges(
            "sql_execution",
            self._route_after_sql_execution,
            {
                "result_validation": "result_validation",
                "error_analysis": "error_analysis",
                "error": "finalize"
            }
        )
        
        workflow.add_edge("finalize", END)
        
        # 编译图（添加内存检查点）
        memory = MemorySaver()
        compiled_graph = workflow.compile(checkpointer=memory)
        
        return compiled_graph
    
    def run(self, user_query: str, database_id: str, additional_info: str = "") -> Dict[str, Any]:
        """
        运行SQL生成系统
        
        Args:
            user_query: 用户查询
            database_id: 数据库ID
            additional_info: 额外信息
            
        Returns:
            执行结果
        """
        try:
            # 初始状态
            initial_state = {
                "user_query": user_query,
                "database_id": database_id,
                "additional_info": additional_info,
                "current_step": "start",
                "iteration_count": 0,
                "max_iterations": 5,
                "known_schema": {},
                "current_sql": "",
                "sql_execution_history": [],
                "last_error": "",
                "error_count": 0,
                "final_sql": "",
                "final_result": [],
                "is_completed": False
            }
            
            # 运行图
            config = {"configurable": {"thread_id": "sql_generation_session"}}
            result = self.graph.invoke(initial_state, config)
            
            return {
                "success": result["is_completed"],
                "final_sql": result["final_sql"],
                "final_result": result["final_result"],
                "iterations": result["iteration_count"],
                "schema_discovered": result["known_schema"],
                "execution_history": result["sql_execution_history"],
                "error_info": result["last_error"] if result["error_count"] > 0 else None
            }
            
        except Exception as e:
            self.logger.error(f"系统运行失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "final_sql": "",
                "final_result": [],
                "iterations": 0
            }
    
    # 节点实现函数
    def _initialize_node(self, state: GraphSystemState) -> GraphSystemState:
        """初始化节点"""
        try:
            self.logger.info(f"开始处理用户查询: {state['user_query']}")
            
            return {
                "current_step": "initialized",
                "iteration_count": 0,
                "error_count": 0,
                "is_completed": False
            }
            
        except Exception as e:
            self.logger.error(f"初始化失败: {e}")
            return {
                "current_step": "error",
                "last_error": str(e),
                "error_count": state["error_count"] + 1
            }
    
    def _info_exploration_node(self, state: GraphSystemState) -> GraphSystemState:
        """信息探索节点（InfoAgent）"""
        try:
            self.logger.info("InfoAgent开始Schema探索")
            
            # 创建信息请求
            if not state["known_schema"]:
                # 初始Schema探索
                request = {
                    "message_type": InteractionType.INITIAL_SCHEMA,
                    "content": "初始Schema信息请求",
                    "metadata": {},
                    "timestamp": None,
                    "query_context": state["user_query"],
                    "error_info": None,
                    "specific_tables": [],
                    "specific_fields": []
                }
            elif state["last_error"]:
                # 基于错误的信息补全
                request = {
                    "message_type": InteractionType.ERROR_FEEDBACK,
                    "content": "基于错误的Schema补全",
                    "metadata": {},
                    "timestamp": None,
                    "query_context": state["user_query"],
                    "error_info": state["last_error"],
                    "specific_tables": [],
                    "specific_fields": []
                }
            else:
                # 其他情况的探索
                request = {
                    "message_type": InteractionType.GLOBAL_SUMMARY,
                    "content": "全局Schema信息",
                    "metadata": {},
                    "timestamp": None,
                    "query_context": state["user_query"],
                    "error_info": None,
                    "specific_tables": [],
                    "specific_fields": []
                }
            
            # 构建SystemState用于InfoAgent
            system_state = {
                'user_query': state['user_query'],
                'database_id': state['database_id'],
                'known_schema': state['known_schema'],
                'current_sql': state.get('current_sql', ''),
                'last_error': state.get('last_error', '')
            }
            
            # 调用InfoAgent
            response = self.info_agent.process_info_request(system_state, request)
            
            self.logger.info("Schema探索完成")
            
            # 使用reducer来更新状态
            return {
                "current_step": "info_explored",
                "known_schema": response["tables_info"],  # 使用dict_merge_reducer合并
                "last_error": ""  # 清除错误
            }
            
        except Exception as e:
            self.logger.error(f"Schema探索失败: {e}")
            return {
                "current_step": "error",
                "last_error": str(e),
                "error_count": state["error_count"] + 1
            }
    
    def _sql_generation_node(self, state: GraphSystemState) -> Union[Command[Literal["sql_execution", "info_exploration", "finalize"]], GraphSystemState]:
        """SQL生成节点（SQLAgent）- 使用Command同时处理状态更新和路由"""
        try:
            self.logger.info("SQLAgent开始生成SQL")
            
            # 构建SystemState用于SQLAgent
            system_state = {
                'user_query': state['user_query'],
                'database_id': state['database_id'],
                'known_schema': state['known_schema'],
                'sql_execution_results': state['sql_execution_history']
            }
            
            # 生成SQL
            sql_query = self.sql_agent.generate_sql(system_state, state["known_schema"])
            
            # 验证SQL质量
            quality_check = self.sql_agent.validate_sql_quality(sql_query, state["user_query"])
            
            update_data = {
                "current_step": "sql_generated",
                "current_sql": sql_query,
                "iteration_count": state["iteration_count"] + 1
            }
            
            if not quality_check["is_acceptable"]:
                self.logger.warning(f"SQL质量检查未通过: {quality_check['issues']}")
                update_data["last_error"] = f"SQL质量问题: {'; '.join(quality_check['issues'])}"
                
                # 如果质量检查失败且可以重试，重新探索信息
                if state["iteration_count"] < state["max_iterations"] and state["error_count"] < 3:
                    return Command(
                        update=update_data,
                        goto="info_exploration"
                    )
                else:
                    # 达到重试上限，结束
                    return Command(
                        update=update_data,
                        goto="finalize"
                    )
            else:
                # SQL质量检查通过，继续执行
                self.logger.info(f"SQL生成完成: {sql_query[:100]}...")
                return Command(
                    update=update_data,
                    goto="sql_execution"
                )
            
        except Exception as e:
            self.logger.error(f"SQL生成失败: {e}")
            return Command(
                update={
                    "current_step": "error",
                    "last_error": str(e),
                    "error_count": state["error_count"] + 1
                },
                goto="finalize"
            )
    
    def _sql_execution_node(self, state: GraphSystemState) -> GraphSystemState:
        """SQL执行节点"""
        try:
            self.logger.info("开始执行SQL")
            
            # 执行SQL
            execution_result = self.sql_agent.execute_sql(
                state["current_sql"], 
                state["database_id"]
            )
            
            result = {
                "current_step": "sql_executed",
                "sql_execution_history": execution_result  # 使用list_append_reducer追加
            }
            
            if execution_result.success:
                self.logger.info(f"SQL执行成功，返回 {len(execution_result.result_data or [])} 行数据")
                result["final_result"] = execution_result.result_data or []
            else:
                self.logger.error(f"SQL执行失败: {execution_result.error_message}")
                result["last_error"] = execution_result.error_message
                result["error_count"] = state["error_count"] + 1
            
            return result
            
        except Exception as e:
            self.logger.error(f"SQL执行过程失败: {e}")
            return {
                "current_step": "error",
                "last_error": str(e),
                "error_count": state["error_count"] + 1
            }
    
    def _error_analysis_node(self, state: GraphSystemState) -> Union[Command[Literal["info_exploration", "sql_generation", "finalize"]], GraphSystemState]:
        """错误分析节点 - 使用Command同时处理状态更新和路由"""
        try:
            self.logger.info("开始分析SQL执行错误")
            
            # 获取最后的执行结果
            last_execution = state["sql_execution_history"][-1] if state["sql_execution_history"] else None
            
            if last_execution and not last_execution.success:
                # 分析执行结果
                analysis = self.sql_agent.analyze_execution_result(
                    last_execution, 
                    state["user_query"]
                )
                
                update_data = {
                    "current_step": "error_analyzed",
                    "last_error": last_execution.error_message
                }
                
                # 根据分析结果决定下一步路由
                if analysis["needs_retry"] and state["iteration_count"] < state["max_iterations"]:
                    self.logger.info("错误分析完成，将请求更多Schema信息")
                    
                    # 根据错误类型决定路由
                    if "table" in last_execution.error_message.lower() or "column" in last_execution.error_message.lower():
                        next_node = "info_exploration"
                    else:
                        next_node = "sql_generation"
                    
                    return Command(
                        update=update_data,
                        goto=next_node
                    )
                else:
                    self.logger.warning("达到最大重试次数或无法修复错误")
                    update_data["is_completed"] = True
                    
                    return Command(
                        update=update_data,
                        goto="finalize"
                    )
            else:
                # 没有错误需要分析，直接完成
                return Command(
                    update={"current_step": "no_error_to_analyze"},
                    goto="finalize"
                )
            
        except Exception as e:
            self.logger.error(f"错误分析失败: {e}")
            return Command(
                update={
                    "current_step": "error",
                    "last_error": str(e),
                    "error_count": state["error_count"] + 1
                },
                goto="finalize"
            )
    
    def _result_validation_node(self, state: GraphSystemState) -> Union[Command[Literal["finalize", "info_exploration", "sql_generation"]], GraphSystemState]:
        """结果验证节点 - 使用Command同时处理状态更新和路由"""
        try:
            self.logger.info("开始验证SQL执行结果")
            
            # 获取最后的执行结果
            last_execution = state["sql_execution_history"][-1] if state["sql_execution_history"] else None
            
            if last_execution and last_execution.success:
                # 分析结果
                analysis = self.sql_agent.analyze_execution_result(
                    last_execution, 
                    state["user_query"]
                )
                
                update_data = {
                    "current_step": "result_validated",
                    "final_sql": state["current_sql"],
                    "final_result": last_execution.result_data or []
                }
                
                # 检查结果是否为空且需要重试
                if (analysis["semantic_validation"] == "empty_result" and 
                    analysis["needs_retry"] and 
                    state["iteration_count"] < state["max_iterations"]):
                    
                    self.logger.info("结果为空，将尝试优化查询")
                    update_data["last_error"] = "查询结果为空，可能需要调整查询条件"
                    
                    return Command(
                        update=update_data,
                        goto="info_exploration"
                    )
                else:
                    # 结果验证通过或达到重试上限
                    self.logger.info("结果验证完成")
                    update_data["is_completed"] = True
                    
                    return Command(
                        update=update_data,
                        goto="finalize"
                    )
            else:
                # 没有成功的执行结果需要验证
                return Command(
                    update={"current_step": "no_result_to_validate"},
                    goto="finalize"
                )
            
        except Exception as e:
            self.logger.error(f"结果验证失败: {e}")
            return Command(
                update={
                    "current_step": "error",
                    "last_error": str(e),
                    "error_count": state["error_count"] + 1
                },
                goto="finalize"
            )
    
    def _finalize_node(self, state: GraphSystemState) -> GraphSystemState:
        """完成节点"""
        try:
            self.logger.info("系统处理完成")
            
            result = {
                "current_step": "finalized",
                "is_completed": True
            }
            
            # 确保最终SQL和结果被设置
            if state["sql_execution_history"]:
                last_successful = None
                for execution in reversed(state["sql_execution_history"]):
                    if execution.success:
                        last_successful = execution
                        break
                
                if last_successful:
                    result["final_sql"] = last_successful.sql_query
                    result["final_result"] = last_successful.result_data or []
                    
            return result
            
        except Exception as e:
            self.logger.error(f"系统完成处理失败: {e}")
            return {
                "current_step": "error",
                "last_error": str(e),
                "is_completed": True  # 强制完成
            }
    
    # 路由函数
    def _route_after_initialize(self, state: GraphSystemState) -> Literal["info_exploration", "error"]:
        """初始化后的路由"""
        if state["current_step"] == "error":
            return "error"
        return "info_exploration"
    
    def _route_after_info_exploration(self, state: GraphSystemState) -> Literal["sql_generation", "continue_exploration", "error"]:
        """信息探索后的路由"""
        if state["current_step"] == "error":
            return "error"
        
        # 如果有足够的Schema信息，进入SQL生成
        if state["known_schema"]:
            return "sql_generation"
        
        # 如果需要继续探索但已达到上限
        if state["iteration_count"] >= state["max_iterations"]:
            return "error"
        
        return "continue_exploration"
    
# _route_after_sql_generation 不再需要，因为使用Command处理路由
    
    def _route_after_sql_execution(self, state: GraphSystemState) -> Literal["result_validation", "error_analysis", "error"]:
        """SQL执行后的路由"""
        if state["current_step"] == "error":
            return "error"
        
        # 检查最后的执行结果
        if state["sql_execution_history"]:
            last_execution = state["sql_execution_history"][-1]
            if last_execution.success:
                return "result_validation"
            else:
                return "error_analysis"
        
        return "error"
    