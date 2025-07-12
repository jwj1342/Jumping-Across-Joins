"""
Snowflake数据库连接模块
支持连接池和重试机制

"""

import snowflake.connector
import os
from typing import Optional, Dict, Any, List
import logging
from dotenv import load_dotenv

# 导入连接池模块
try:
    from utils.SnowflakeConnectionPool import snowflake_sql_query_with_pool, get_pool_stats, close_global_pool
    _HAS_POOL = True
except ImportError:
    _HAS_POOL = False


def snowflake_sql_query(
    sql_query: str, database_id: str, timeout: int = 30, log: bool = False, use_pool: bool = True
) -> List[Dict[str, Any]]:
    """
    执行Snowflake SQL查询并返回结果

    参数:
        sql_query (str): 要执行的SQL查询语句
        database_id (str): 数据库标识符
        timeout (int): 连接超时时间，默认30秒
        log (bool): 是否输出SQL查询语句日志，默认为False
        use_pool (bool): 是否使用连接池，默认为True

    返回:
        List[Dict[str, Any]]: 查询结果，每行数据作为字典返回

    异常:
        ValueError: SQL查询为空时抛出
        ConnectionError: 连接失败时抛出
        Exception: 执行查询时发生错误
    """
    if not sql_query or not sql_query.strip():
        raise ValueError("SQL查询不能为空")

    if not database_id or not database_id.strip():
        raise ValueError("数据库ID不能为空")
    
    # 如果可以使用连接池且启用了连接池，则使用连接池
    if _HAS_POOL and use_pool:
        try:
            return snowflake_sql_query_with_pool(
                sql_query=sql_query,
                database_id=database_id,
                timeout=timeout,
                log=log
            )
        except Exception as e:
            # 如果连接池失败，回退到原始连接方式
            logging.warning(f"连接池查询失败，回退到原始连接: {e}")
            pass  # 继续执行原始连接逻辑

    # 加载环境变量
    load_dotenv(".env")

    # 从环境变量获取连接参数
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    account = os.getenv("SNOWFLAKE_ACCOUNT")

    # 检查必需的连接参数
    if not user:
        raise ConnectionError("未找到SNOWFLAKE_USER环境变量")
    if not password:
        raise ConnectionError("未找到SNOWFLAKE_PASSWORD环境变量")
    if not account:
        raise ConnectionError("未找到SNOWFLAKE_ACCOUNT环境变量")

    # 连接参数（优化超时设置）
    connection_params = {
        "user": user,
        "password": password,
        "account": account,
        "database": database_id,
        "login_timeout": min(timeout, 60),  # 登录超时最大60秒
        "network_timeout": timeout,
        "socket_timeout": timeout,
        "application": "Schema_Extractor",  # 标识应用
        "session_parameters": {
            "QUERY_TIMEOUT": timeout,  # 查询级别超时
            "STATEMENT_TIMEOUT_IN_SECONDS": timeout,
        },
    }

    conn = None
    cursor = None

    try:
        if log:
            logging.info(f"正在连接到Snowflake数据库: {database_id}")
            logging.info(f"执行SQL查询: {sql_query}")

        # 建立连接
        conn = snowflake.connector.connect(**connection_params)

        # 创建游标并执行查询
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

        logging.info(f"查询完成，返回{len(results)}行数据")
        return results

    except snowflake.connector.Error as e:
        logging.error(f"Snowflake连接或查询错误: {e}")
        raise Exception(f"执行Snowflake查询时发生错误: {e}")

    except Exception as e:
        logging.error(f"执行查询时发生未知错误: {e}")
        raise

    finally:
        # 清理资源
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    """
    用于测试snowflake_sql_query函数
    """

    print("=== Snowflake SQL查询测试 ===\n")

    try:
        # 测试1: 基本查询测试
        print("1. 测试基本查询...")
        test_database = "GA360"
        # 测试简单查询
        results = snowflake_sql_query(
            sql_query="SELECT CURRENT_VERSION() as version, CURRENT_USER() as user",
            database_id=test_database,
            timeout=30,
        )

        print("✓ 查询成功!")
        print("结果:")
        for result in results:
            for key, value in result.items():
                print(f"  {key}: {value}")

        print("\n" + "=" * 50 + "\n")

        # 测试2: 时间查询
        print("2. 测试时间查询...")
        results = snowflake_sql_query(
            sql_query="SELECT CURRENT_TIMESTAMP() as current_time",
            database_id=test_database,
        )

        print("✓ 时间查询成功!")
        print("结果:")
        for result in results:
            print(f"  当前时间: {result.get('CURRENT_TIME')}")

        print("\n" + "=" * 50 + "\n")

        # 测试3: 数据库信息查询
        print("3. 测试数据库信息查询...")
        results = snowflake_sql_query(
            sql_query="SELECT * FROM CRYPTO.INFORMATION_SCHEMA.COLUMNS;",
            database_id=test_database,
        )

        print("✓ 数据库信息查询成功!")
        print("\n✓ 所有测试通过!")

    except ValueError as e:
        print(f"✗ 参数错误: {e}")
    except ConnectionError as e:
        print(f"✗ 连接错误: {e}")
    except Exception as e:
        print(f"✗ 执行错误: {e}")

    # 测试错误处理
    print("\n" + "=" * 50)
    print("=== 错误处理测试 ===\n")

    # 测试空查询
    print("4. 测试空查询错误处理...")
    try:
        snowflake_sql_query("", "TEST_DB")
        print("✗ 应该抛出ValueError")
    except ValueError as e:
        print(f"✓ 正确捕获空查询错误: {e}")

    # 测试空数据库ID
    print("\n5. 测试空数据库ID错误处理...")
    try:
        snowflake_sql_query("SELECT 1", "")
        print("✗ 应该抛出ValueError")
    except ValueError as e:
        print(f"✓ 正确捕获空数据库ID错误: {e}")

    print("\n=== 测试完成 ===")
