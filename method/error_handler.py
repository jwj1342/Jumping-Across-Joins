"""
错误处理和工具模块
提供统一的错误处理、重试机制和系统监控功能
"""

import logging
import time
import functools
from typing import Any, Callable, Dict, Optional, Tuple, Union
from enum import Enum
import traceback


class ErrorType(Enum):
    """错误类型枚举"""
    NETWORK_ERROR = "network_error"
    DATABASE_ERROR = "database_error"
    SQL_SYNTAX_ERROR = "sql_syntax_error"
    SCHEMA_ERROR = "schema_error"
    LLM_ERROR = "llm_error"
    VALIDATION_ERROR = "validation_error"
    TIMEOUT_ERROR = "timeout_error"
    UNKNOWN_ERROR = "unknown_error"


class SystemError(Exception):
    """系统自定义异常"""
    
    def __init__(self, message: str, error_type: ErrorType = ErrorType.UNKNOWN_ERROR, 
                 details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.error_type = error_type
        self.details = details or {}
        self.timestamp = time.time()


class ErrorHandler:
    """错误处理器"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.error_counts = {}
        self.error_history = []
        
    def classify_error(self, error: Exception) -> ErrorType:
        """
        错误分类
        
        Args:
            error: 异常对象
            
        Returns:
            错误类型
        """
        error_message = str(error).lower()
        
        # 网络相关错误
        if any(keyword in error_message for keyword in [
            'connection', 'timeout', 'network', 'unreachable', 'refused'
        ]):
            return ErrorType.NETWORK_ERROR
        
        # 数据库相关错误
        if any(keyword in error_message for keyword in [
            'database', 'snowflake', 'sql', 'table', 'column', 'syntax'
        ]):
            if any(keyword in error_message for keyword in ['syntax', 'invalid']):
                return ErrorType.SQL_SYNTAX_ERROR
            elif any(keyword in error_message for keyword in ['table', 'column', 'schema']):
                return ErrorType.SCHEMA_ERROR
            else:
                return ErrorType.DATABASE_ERROR
        
        # LLM相关错误
        if any(keyword in error_message for keyword in [
            'openai', 'api', 'token', 'model', 'generation'
        ]):
            return ErrorType.LLM_ERROR
        
        # 超时错误
        if 'timeout' in error_message:
            return ErrorType.TIMEOUT_ERROR
        
        # 验证错误
        if any(keyword in error_message for keyword in [
            'validation', 'invalid', 'format', 'type'
        ]):
            return ErrorType.VALIDATION_ERROR
        
        return ErrorType.UNKNOWN_ERROR
    
    def handle_error(self, error: Exception, context: str = "") -> SystemError:
        """
        处理错误
        
        Args:
            error: 原始异常
            context: 错误上下文
            
        Returns:
            系统异常对象
        """
        error_type = self.classify_error(error)
        
        # 记录错误
        self.error_counts[error_type] = self.error_counts.get(error_type, 0) + 1
        
        # 创建错误详情
        details = {
            'original_error': str(error),
            'error_class': error.__class__.__name__,
            'context': context,
            'traceback': traceback.format_exc(),
            'count': self.error_counts[error_type]
        }
        
        # 创建系统异常
        system_error = SystemError(
            message=f"{context}: {str(error)}" if context else str(error),
            error_type=error_type,
            details=details
        )
        
        # 记录到历史
        self.error_history.append(system_error)
        
        # 记录日志
        self.logger.error(f"[{error_type.value}] {system_error.message}")
        self.logger.debug(f"错误详情: {details}")
        
        return system_error
    
    def get_error_statistics(self) -> Dict[str, Any]:
        """获取错误统计信息"""
        return {
            'error_counts': dict(self.error_counts),
            'total_errors': len(self.error_history),
            'recent_errors': [
                {
                    'type': err.error_type.value,
                    'message': err.message,
                    'timestamp': err.timestamp
                }
                for err in self.error_history[-10:]  # 最近10个错误
            ]
        }
    
    def should_retry(self, error_type: ErrorType, attempt: int, max_attempts: int = 3) -> bool:
        """
        判断是否应该重试
        
        Args:
            error_type: 错误类型
            attempt: 当前尝试次数
            max_attempts: 最大尝试次数
            
        Returns:
            是否应该重试
        """
        if attempt >= max_attempts:
            return False
        
        # 根据错误类型决定是否重试
        retry_types = {
            ErrorType.NETWORK_ERROR: True,
            ErrorType.TIMEOUT_ERROR: True,
            ErrorType.DATABASE_ERROR: True,
            ErrorType.LLM_ERROR: True,
            ErrorType.SQL_SYNTAX_ERROR: False,  # 语法错误不重试
            ErrorType.VALIDATION_ERROR: False,  # 验证错误不重试
            ErrorType.SCHEMA_ERROR: True,  # Schema错误可以重试
            ErrorType.UNKNOWN_ERROR: True
        }
        
        return retry_types.get(error_type, False)
    
    def get_retry_delay(self, error_type: ErrorType, attempt: int) -> float:
        """
        获取重试延迟时间
        
        Args:
            error_type: 错误类型
            attempt: 尝试次数
            
        Returns:
            延迟秒数
        """
        base_delays = {
            ErrorType.NETWORK_ERROR: 2.0,
            ErrorType.TIMEOUT_ERROR: 1.0,
            ErrorType.DATABASE_ERROR: 3.0,
            ErrorType.LLM_ERROR: 5.0,
            ErrorType.SCHEMA_ERROR: 1.0,
            ErrorType.UNKNOWN_ERROR: 2.0
        }
        
        base_delay = base_delays.get(error_type, 2.0)
        
        # 指数退避
        return base_delay * (2 ** (attempt - 1))


def retry_on_error(max_attempts: int = 3, 
                  error_handler: Optional[ErrorHandler] = None):
    """
    重试装饰器
    
    Args:
        max_attempts: 最大尝试次数
        error_handler: 错误处理器
    """
    if error_handler is None:
        error_handler = ErrorHandler()
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                    
                except Exception as e:
                    last_error = error_handler.handle_error(
                        e, 
                        f"{func.__name__} (尝试 {attempt}/{max_attempts})"
                    )
                    
                    if not error_handler.should_retry(last_error.error_type, attempt, max_attempts):
                        break
                    
                    if attempt < max_attempts:
                        delay = error_handler.get_retry_delay(last_error.error_type, attempt)
                        error_handler.logger.info(f"等待 {delay:.1f}秒后重试...")
                        time.sleep(delay)
            
            # 所有尝试都失败了
            raise last_error
        
        return wrapper
    return decorator


def safe_execute(func: Callable, *args, **kwargs) -> Tuple[bool, Any]:
    """
    安全执行函数
    
    Args:
        func: 要执行的函数
        *args: 位置参数
        **kwargs: 关键字参数
        
    Returns:
        (是否成功, 结果或错误)
    """
    try:
        result = func(*args, **kwargs)
        return True, result
    except Exception as e:
        return False, e


class PerformanceMonitor:
    """性能监控器"""
    
    def __init__(self):
        self.metrics = {}
        self.logger = logging.getLogger(__name__)
    
    def record_execution_time(self, operation: str, execution_time: float):
        """记录执行时间"""
        if operation not in self.metrics:
            self.metrics[operation] = {
                'count': 0,
                'total_time': 0.0,
                'min_time': float('inf'),
                'max_time': 0.0,
                'avg_time': 0.0
            }
        
        metrics = self.metrics[operation]
        metrics['count'] += 1
        metrics['total_time'] += execution_time
        metrics['min_time'] = min(metrics['min_time'], execution_time)
        metrics['max_time'] = max(metrics['max_time'], execution_time)
        metrics['avg_time'] = metrics['total_time'] / metrics['count']
        
        self.logger.debug(f"操作 {operation} 耗时: {execution_time:.2f}秒")
    
    def get_performance_report(self) -> Dict[str, Any]:
        """获取性能报告"""
        return dict(self.metrics)


def monitor_performance(monitor: Optional[PerformanceMonitor] = None):
    """性能监控装饰器"""
    if monitor is None:
        monitor = PerformanceMonitor()
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                monitor.record_execution_time(func.__name__, execution_time)
                return result
            except Exception as e:
                execution_time = time.time() - start_time
                monitor.record_execution_time(f"{func.__name__}_failed", execution_time)
                raise
        
        return wrapper
    return decorator


class SystemHealthChecker:
    """系统健康检查器"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.last_check_time = 0
        self.check_interval = 300  # 5分钟检查一次
    
    def check_database_connection(self, database_id: str) -> bool:
        """检查数据库连接"""
        try:
            from utils.SnowConnect import snowflake_sql_query
            
            test_sql = "SELECT 1 as test"
            result = snowflake_sql_query(test_sql, database_id, timeout=10)
            return len(result) > 0
            
        except Exception as e:
            self.logger.error(f"数据库连接检查失败: {e}")
            return False
    
    def check_graph_database_connection(self) -> bool:
        """检查图数据库连接"""
        try:
            from utils.CypherExecutor import CypherExecutor
            
            executor = CypherExecutor()
            return executor.verify_connectivity()
            
        except Exception as e:
            self.logger.error(f"图数据库连接检查失败: {e}")
            return False
    
    def check_llm_connection(self) -> bool:
        """检查LLM连接"""
        try:
            from utils.init_llm import initialize_llm
            
            llm = initialize_llm()
            if llm:
                # 简单测试
                response = llm.invoke("test")
                return bool(response)
            return False
            
        except Exception as e:
            self.logger.error(f"LLM连接检查失败: {e}")
            return False
    
    def perform_health_check(self, database_id: str = "CRYPTO") -> Dict[str, Any]:
        """执行健康检查"""
        current_time = time.time()
        
        # 如果距离上次检查时间太短，跳过
        if current_time - self.last_check_time < self.check_interval:
            return {"status": "skipped", "reason": "检查间隔未到"}
        
        self.last_check_time = current_time
        
        health_status = {
            "timestamp": current_time,
            "database": self.check_database_connection(database_id),
            "graph_database": self.check_graph_database_connection(),
            "llm": self.check_llm_connection(),
            "overall": True
        }
        
        # 计算整体状态
        health_status["overall"] = all([
            health_status["database"],
            health_status["graph_database"],
            health_status["llm"]
        ])
        
        if not health_status["overall"]:
            self.logger.warning("系统健康检查发现问题")
        else:
            self.logger.info("系统健康检查正常")
        
        return health_status


# 全局实例
global_error_handler = ErrorHandler()
global_performance_monitor = PerformanceMonitor()
global_health_checker = SystemHealthChecker() 