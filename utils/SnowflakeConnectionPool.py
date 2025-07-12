"""
Snowflake连接池管理模块
实现连接池、重试机制和连接健康检查
"""

import snowflake.connector
import os
import time
import logging
import threading
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv
from queue import Queue, Empty
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
import random


class ConnectionStatus(Enum):
    """连接状态枚举"""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class ConnectionInfo:
    """连接信息"""
    connection: snowflake.connector.SnowflakeConnection
    created_at: float
    last_used: float
    status: ConnectionStatus = ConnectionStatus.UNKNOWN
    use_count: int = 0


class SnowflakeConnectionPool:
    """Snowflake连接池管理器"""
    
    def __init__(self, 
                 max_connections: int = 16,
                 min_connections: int = 2,
                 connection_timeout: int = 60,
                 max_connection_age: int = 3600,  # 1小时
                 health_check_interval: int = 300,  # 5分钟
                 max_retries: int = 3,
                 retry_delay: float = 1.0,
                 retry_backoff: float = 2.0):
        """
        初始化连接池
        
        Args:
            max_connections: 最大连接数
            min_connections: 最小连接数
            connection_timeout: 连接超时时间（秒）
            max_connection_age: 连接最大存活时间（秒）
            health_check_interval: 健康检查间隔（秒）
            max_retries: 最大重试次数
            retry_delay: 重试延迟（秒）
            retry_backoff: 重试退避倍数
        """
        self.max_connections = max_connections
        self.min_connections = min_connections
        self.connection_timeout = connection_timeout
        self.max_connection_age = max_connection_age
        self.health_check_interval = health_check_interval
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.retry_backoff = retry_backoff
        
        # 连接池和锁
        self._pool = Queue(maxsize=max_connections)
        self._pool_lock = threading.Lock()
        self._active_connections = {}  # 活跃连接字典
        self._connection_count = 0
        self._closed = False
        
        # 统计信息
        self._stats = {
            'total_created': 0,
            'total_destroyed': 0,
            'total_borrowed': 0,
            'total_returned': 0,
            'total_health_checks': 0,
            'total_retries': 0,
            'current_active': 0
        }
        
        # 加载环境变量
        load_dotenv(".env")
        
        # 获取连接参数
        self._connection_params = self._get_connection_params()
        
        # 启动健康检查线程
        self._health_check_thread = threading.Thread(
            target=self._health_check_worker, 
            daemon=True
        )
        self._health_check_thread.start()
        
        # 预创建最小连接数
        self._initialize_pool()
        
        logging.info(f"Snowflake连接池已初始化: max={max_connections}, min={min_connections}")
    
    def _get_connection_params(self) -> Dict[str, Any]:
        """获取连接参数"""
        user = os.getenv("SNOWFLAKE_USER")
        password = os.getenv("SNOWFLAKE_PASSWORD")
        account = os.getenv("SNOWFLAKE_ACCOUNT")
        
        if not user:
            raise ConnectionError("未找到SNOWFLAKE_USER环境变量")
        if not password:
            raise ConnectionError("未找到SNOWFLAKE_PASSWORD环境变量")
        if not account:
            raise ConnectionError("未找到SNOWFLAKE_ACCOUNT环境变量")
        
        return {
            "user": user,
            "password": password,
            "account": account,
            "login_timeout": min(self.connection_timeout, 60),
            "network_timeout": self.connection_timeout,
            "socket_timeout": self.connection_timeout,
            "application": "SQL_Generator_Pool",
            "session_parameters": {
                "QUERY_TIMEOUT": self.connection_timeout,
                "STATEMENT_TIMEOUT_IN_SECONDS": self.connection_timeout,
            },
        }
    
    def _create_connection(self, database_id: str) -> snowflake.connector.SnowflakeConnection:
        """创建新连接"""
        try:
            params = self._connection_params.copy()
            params["database"] = database_id
            
            conn = snowflake.connector.connect(**params)
            self._stats['total_created'] += 1
            logging.debug(f"创建新的Snowflake连接: {database_id}")
            return conn
            
        except Exception as e:
            logging.error(f"创建Snowflake连接失败: {e}")
            raise
    
    def _is_connection_healthy(self, conn: snowflake.connector.SnowflakeConnection) -> bool:
        """检查连接健康状态"""
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except Exception as e:
            logging.debug(f"连接健康检查失败: {e}")
            return False
    
    def _close_connection(self, conn: snowflake.connector.SnowflakeConnection):
        """关闭连接"""
        try:
            conn.close()
            self._stats['total_destroyed'] += 1
            logging.debug("连接已关闭")
        except Exception as e:
            logging.warning(f"关闭连接时出错: {e}")
    
    def _initialize_pool(self):
        """初始化连接池"""
        # 这里不预创建连接，因为需要database_id
        # 连接将在首次使用时创建
        pass
    
    def _health_check_worker(self):
        """健康检查工作线程"""
        while not self._closed:
            try:
                time.sleep(self.health_check_interval)
                if self._closed:
                    break
                
                self._perform_health_check()
                
            except Exception as e:
                logging.error(f"健康检查线程错误: {e}")
    
    def _perform_health_check(self):
        """执行健康检查"""
        current_time = time.time()
        connections_to_remove = []
        
        with self._pool_lock:
            # 检查池中的连接
            temp_connections = []
            while not self._pool.empty():
                try:
                    conn_info = self._pool.get_nowait()
                    
                    # 检查连接年龄
                    if current_time - conn_info.created_at > self.max_connection_age:
                        connections_to_remove.append(conn_info)
                        continue
                    
                    # 检查连接健康状态
                    if self._is_connection_healthy(conn_info.connection):
                        conn_info.status = ConnectionStatus.HEALTHY
                        temp_connections.append(conn_info)
                    else:
                        conn_info.status = ConnectionStatus.UNHEALTHY
                        connections_to_remove.append(conn_info)
                        
                except Empty:
                    break
            
            # 将健康的连接放回池中
            for conn_info in temp_connections:
                self._pool.put(conn_info)
        
        # 关闭不健康的连接
        for conn_info in connections_to_remove:
            self._close_connection(conn_info.connection)
            self._connection_count -= 1
        
        if connections_to_remove:
            logging.info(f"健康检查完成，移除了 {len(connections_to_remove)} 个不健康连接")
        
        self._stats['total_health_checks'] += 1
    
    @contextmanager
    def get_connection(self, database_id: str):
        """
        获取连接的上下文管理器
        
        Args:
            database_id: 数据库ID
            
        Yields:
            snowflake.connector.SnowflakeConnection: 数据库连接
        """
        conn_info = None
        try:
            conn_info = self._borrow_connection(database_id)
            yield conn_info.connection
        finally:
            if conn_info:
                self._return_connection(conn_info)
    
    def _borrow_connection(self, database_id: str) -> ConnectionInfo:
        """借用连接"""
        if self._closed:
            raise RuntimeError("连接池已关闭")
        
        # 尝试从池中获取连接
        try:
            conn_info = self._pool.get_nowait()
            
            # 检查连接是否健康
            if self._is_connection_healthy(conn_info.connection):
                conn_info.last_used = time.time()
                conn_info.use_count += 1
                self._stats['total_borrowed'] += 1
                self._stats['current_active'] += 1
                
                # 将连接添加到活跃连接字典
                conn_id = id(conn_info.connection)
                self._active_connections[conn_id] = conn_info
                
                logging.debug(f"从池中借用连接: {database_id}")
                return conn_info
            else:
                # 连接不健康，关闭并创建新连接
                self._close_connection(conn_info.connection)
                self._connection_count -= 1
                
        except Empty:
            # 池中没有可用连接
            pass
        
        # 创建新连接
        if self._connection_count < self.max_connections:
            conn = self._create_connection(database_id)
            conn_info = ConnectionInfo(
                connection=conn,
                created_at=time.time(),
                last_used=time.time(),
                status=ConnectionStatus.HEALTHY,
                use_count=1
            )
            
            self._connection_count += 1
            self._stats['total_borrowed'] += 1
            self._stats['current_active'] += 1
            
            # 将连接添加到活跃连接字典
            conn_id = id(conn_info.connection)
            self._active_connections[conn_id] = conn_info
            
            logging.debug(f"创建新连接: {database_id}")
            return conn_info
        else:
            raise RuntimeError("连接池已满，无法创建新连接")
    
    def _return_connection(self, conn_info: ConnectionInfo):
        """归还连接"""
        try:
            conn_id = id(conn_info.connection)
            
            # 从活跃连接字典中移除
            if conn_id in self._active_connections:
                del self._active_connections[conn_id]
            
            self._stats['current_active'] -= 1
            
            # 检查连接是否仍然健康
            if (self._is_connection_healthy(conn_info.connection) and 
                time.time() - conn_info.created_at < self.max_connection_age):
                
                # 将连接放回池中
                try:
                    self._pool.put_nowait(conn_info)
                    self._stats['total_returned'] += 1
                    logging.debug("连接已归还到池中")
                except:
                    # 池已满，关闭连接
                    self._close_connection(conn_info.connection)
                    self._connection_count -= 1
            else:
                # 连接不健康或过期，关闭它
                self._close_connection(conn_info.connection)
                self._connection_count -= 1
                
        except Exception as e:
            logging.error(f"归还连接时出错: {e}")
    
    def _is_retryable_error(self, error: Exception) -> bool:
        """
        判断错误是否可重试
        只有连接相关的错误才进行重试，其他错误直接抛出
        
        Args:
            error: 异常对象
            
        Returns:
            是否可重试
        """
        error_message = str(error).lower()
        
        # 连接相关的错误，可以重试
        retryable_keywords = [
            'timeout', 'timed out', 'read timeout', 'connection timeout',
            'connection reset', 'connection refused', 'connection failed',
            'network', 'unreachable', 'connection error', 'socket timeout',
            'broken pipe', 'connection aborted', 'connection lost',
            'temporary failure', 'service unavailable', 'server error',
            'internal server error', '500', '502', '503', '504',
            'retry', 'throttled', 'rate limit', 'too many requests'
        ]
        
        # 检查是否包含可重试的关键词
        for keyword in retryable_keywords:
            if keyword in error_message:
                return True
        
        # 检查具体的异常类型
        if hasattr(error, '__class__'):
            error_type = error.__class__.__name__.lower()
            retryable_types = [
                'timeout', 'connectionerror', 'networkerror', 'readtimeout',
                'connectiontimeout', 'sockettimeout', 'httperror'
            ]
            
            for error_type_keyword in retryable_types:
                if error_type_keyword in error_type:
                    return True
        
        # 非连接相关错误，不重试
        return False

    def execute_query_with_retry(self, 
                                sql_query: str, 
                                database_id: str, 
                                log: bool = False) -> List[Dict[str, Any]]:
        """
        执行SQL查询，只对连接相关错误进行重试
        
        Args:
            sql_query: SQL查询语句
            database_id: 数据库ID
            log: 是否记录日志
            
        Returns:
            查询结果列表
        """
        if not sql_query or not sql_query.strip():
            raise ValueError("SQL查询不能为空")
        
        if not database_id or not database_id.strip():
            raise ValueError("数据库ID不能为空")
        
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                if log and attempt > 0:
                    logging.info(f"重试执行查询 (第{attempt}次): {sql_query[:100]}...")
                elif log:
                    logging.info(f"执行查询: {sql_query[:100]}...")
                
                with self.get_connection(database_id) as conn:
                    cursor = conn.cursor()
                    cursor.execute(sql_query)
                    
                    # 获取列名
                    columns = [desc[0] for desc in cursor.description] if cursor.description else []
                    
                    # 获取所有结果
                    rows = cursor.fetchall()
                    
                    # 将结果转换为字典列表
                    results = []
                    for row in rows:
                        row_dict = {}
                        for i, value in enumerate(row):
                            column_name = columns[i] if i < len(columns) else f"column_{i}"
                            row_dict[column_name] = value
                        results.append(row_dict)
                    
                    cursor.close()
                    
                    if log:
                        logging.info(f"查询完成，返回{len(results)}行数据")
                    
                    return results
                    
            except Exception as e:
                last_exception = e
                
                # 检查是否是可重试的错误
                if not self._is_retryable_error(e):
                    # 非连接相关错误，直接抛出，不重试
                    logging.debug(f"非连接相关错误，不重试: {e}")
                    raise e
                
                # 连接相关错误，进行重试
                self._stats['total_retries'] += 1
                
                if attempt < self.max_retries:
                    # 计算重试延迟（指数退避 + 随机抖动）
                    delay = self.retry_delay * (self.retry_backoff ** attempt)
                    jitter = random.uniform(0, delay * 0.1)  # 10%的抖动
                    total_delay = delay + jitter
                    
                    logging.warning(f"连接相关错误，第{attempt + 1}次尝试失败: {e}, {total_delay:.2f}秒后重试")
                    time.sleep(total_delay)
                else:
                    logging.error(f"连接相关错误，已重试{self.max_retries}次仍失败: {e}")
        
        # 重试耗尽，抛出最后一个异常
        if last_exception:
            raise Exception(f"执行Snowflake查询失败，已重试{self.max_retries}次: {last_exception}")
        else:
            raise Exception("执行Snowflake查询失败，原因未知")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取连接池统计信息"""
        with self._pool_lock:
            return {
                **self._stats,
                'pool_size': self._pool.qsize(),
                'total_connections': self._connection_count,
                'max_connections': self.max_connections,
                'min_connections': self.min_connections
            }
    
    def close(self):
        """关闭连接池"""
        if self._closed:
            return
        
        self._closed = True
        
        # 关闭所有池中的连接
        while not self._pool.empty():
            try:
                conn_info = self._pool.get_nowait()
                self._close_connection(conn_info.connection)
            except Empty:
                break
        
        # 关闭所有活跃连接
        for conn_info in self._active_connections.values():
            self._close_connection(conn_info.connection)
        
        self._active_connections.clear()
        self._connection_count = 0
        
        logging.info("连接池已关闭")


# 全局连接池实例
_global_pool = None
_pool_lock = threading.Lock()


def get_global_pool(max_connections: int = 16) -> SnowflakeConnectionPool:
    """获取全局连接池实例"""
    global _global_pool
    
    if _global_pool is None:
        with _pool_lock:
            if _global_pool is None:
                _global_pool = SnowflakeConnectionPool(max_connections=max_connections)
    
    return _global_pool


def snowflake_sql_query_with_pool(
    sql_query: str, 
    database_id: str, 
    timeout: int = 60, 
    log: bool = False,
    max_connections: int = 16
) -> List[Dict[str, Any]]:
    """
    使用连接池执行Snowflake SQL查询
    
    Args:
        sql_query: SQL查询语句
        database_id: 数据库ID
        timeout: 超时时间
        log: 是否记录日志
        max_connections: 最大连接数
        
    Returns:
        查询结果列表
    """
    pool = get_global_pool(max_connections)
    return pool.execute_query_with_retry(sql_query, database_id, log)


def close_global_pool():
    """关闭全局连接池"""
    global _global_pool
    
    if _global_pool:
        _global_pool.close()
        _global_pool = None


def get_pool_stats() -> Dict[str, Any]:
    """获取连接池统计信息"""
    pool = get_global_pool()
    return pool.get_stats()


if __name__ == "__main__":
    """测试连接池功能"""
    
    print("=== Snowflake连接池测试 ===\n")
    
    try:
        # 测试1: 基本查询测试
        print("1. 测试基本查询...")
        test_database = "GA360"
        
        results = snowflake_sql_query_with_pool(
            sql_query="SELECT CURRENT_VERSION() as version, CURRENT_USER() as user",
            database_id=test_database,
            timeout=30,
            log=True
        )
        
        print("✓ 查询成功!")
        print("结果:")
        for result in results:
            for key, value in result.items():
                print(f"  {key}: {value}")
        
        # 测试2: 并发查询测试
        print("\n2. 测试并发查询...")
        import threading
        import time
        
        def concurrent_query(query_id):
            try:
                results = snowflake_sql_query_with_pool(
                    sql_query=f"SELECT {query_id} as query_id, CURRENT_TIMESTAMP() as ts",
                    database_id=test_database,
                    log=True
                )
                print(f"  查询{query_id}完成: {len(results)}行")
            except Exception as e:
                print(f"  查询{query_id}失败: {e}")
        
        threads = []
        for i in range(5):
            t = threading.Thread(target=concurrent_query, args=(i,))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        print("✓ 并发查询测试完成!")
        
        # 测试3: 连接池统计
        print("\n3. 连接池统计信息:")
        stats = get_pool_stats()
        for key, value in stats.items():
            print(f"  {key}: {value}")
        
        print("\n✓ 所有测试通过!")
        
    except Exception as e:
        print(f"✗ 测试失败: {e}")
    
    finally:
        # 关闭连接池
        close_global_pool()
        print("\n连接池已关闭") 