import logging
import time
from typing import List, Dict, Any, Optional
from SnowConnect import snowflake_sql_query
from sql_templates import (
    GET_ALL_SCHEMAS,
    GET_USER_SCHEMAS,
    GET_SPECIFIC_SCHEMA,
    GET_TABLES_IN_SCHEMA,
    GET_TABLES_BASIC,
    GET_COLUMNS_FOR_TABLE,
    GET_COLUMNS_BASIC,
    GET_CONSTRAINTS_FOR_SCHEMA,
    GET_FOREIGN_KEYS_FOR_SCHEMA,
)


def _retry_query(
    sql_query: str, database_name: str, timeout: int = 60, max_retries: int = 3
) -> List[Dict[str, Any]]:
    """
    带重试机制的查询函数

    Args:
        sql_query: SQL查询语句
        database_name: 数据库名称
        timeout: 超时时间（秒）
        max_retries: 最大重试次数

    Returns:
        查询结果

    Raises:
        Exception: 所有重试都失败时抛出异常
    """
    last_exception = None

    for attempt in range(max_retries):
        try:
            logging.info(f"执行查询（第{attempt + 1}次尝试）: {sql_query[:100]}...")
            result = snowflake_sql_query(
                sql_query=sql_query, database_id=database_name, timeout=timeout
            )
            logging.info(f"查询成功，返回{len(result)}行数据")
            return result

        except Exception as e:
            last_exception = e
            error_msg = str(e)

            # 检查是否是超时错误
            if "timeout" in error_msg.lower() or "000604" in error_msg:
                logging.warning(f"查询超时（第{attempt + 1}次尝试），错误: {error_msg}")
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2  # 递增等待时间
                    logging.info(f"等待{wait_time}秒后重试...")
                    time.sleep(wait_time)
                    timeout = min(timeout * 1.5, 180)  # 增加超时时间，最大3分钟
                    continue
            else:
                logging.error(f"查询出现非超时错误: {error_msg}")
                break

    # 所有重试都失败
    raise Exception(f"查询失败，已重试{max_retries}次。最后错误: {last_exception}")


def _safe_query(
    sql_query: str,
    database_name: str,
    operation_name: str,
    timeout: int = 60,
    fallback_result: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """
    安全查询函数，包含容错机制

    Args:
        sql_query: SQL查询语句
        database_name: 数据库名称
        operation_name: 操作名称（用于日志）
        timeout: 超时时间
        fallback_result: 失败时的降级结果

    Returns:
        查询结果或降级结果
    """
    try:
        return _retry_query(sql_query, database_name, timeout)
    except Exception as e:
        logging.error(f"{operation_name}失败: {e}")
        if fallback_result is not None:
            logging.info(f"使用降级结果: {fallback_result}")
            return fallback_result
        return []


def extract_db_info(
    database_name: str, specific_schema: Optional[str] = None, fast_mode: bool = False
) -> str:
    """
    从数据库中提取完整的schema信息

    按照以下层次结构提取信息：
    1. 获取Schema（所有或指定）
    2. 对每个Schema获取所有表
    3. 对每个表获取列信息
    4. 获取约束信息（主键、唯一约束、外键）

    Args:
        database_name: 数据库名称
        specific_schema: 指定schema名称，如果提供则只提取该schema的信息
        fast_mode: 快速模式，使用更简单的查询以避免超时

    Returns:
        完整的schema信息字符串
    """
    logging.info(f"开始提取数据库 {database_name} 的schema信息")
    if specific_schema:
        logging.info(f"指定Schema: {specific_schema}")
    if fast_mode:
        logging.info("启用快速模式")

    try:
        # 第一步：获取Schema信息
        if specific_schema:
            logging.info(f"步骤1: 获取指定Schema: {specific_schema}")
            schemas_result = _safe_query(
                sql_query=GET_SPECIFIC_SCHEMA.format(schema_name=specific_schema),
                database_name=database_name,
                operation_name=f"获取Schema {specific_schema}",
                timeout=30,
                fallback_result=[],
            )
        else:
            logging.info("步骤1: 获取所有用户Schema")
            # 先尝试获取用户Schema（更安全）
            schemas_result = _safe_query(
                sql_query=GET_USER_SCHEMAS,
                database_name=database_name,
                operation_name="获取用户Schema",
                timeout=60,
                fallback_result=[],
            )

            # 如果用户Schema查询失败，尝试完整查询
            if not schemas_result:
                logging.info("用户Schema查询无结果，尝试完整Schema查询")
                schemas_result = _safe_query(
                    sql_query=GET_ALL_SCHEMAS,
                    database_name=database_name,
                    operation_name="获取所有Schema",
                    timeout=90,
                    fallback_result=[],
                )

        if not schemas_result:
            logging.warning("未找到任何Schema")
            return "未找到任何Schema信息"

        schema_info = []
        schema_info.append(f"Database: {database_name}")
        schema_info.append("=" * 50)

        # 遍历每个Schema
        for schema_row in schemas_result:
            schema_name = schema_row.get("SCHEMA_NAME")
            if not schema_name:
                continue

            logging.info(f"处理Schema: {schema_name}")
            schema_info.append(f"\nSchema: {schema_name}")
            schema_info.append("-" * 30)

            # 第二步：获取该Schema下的所有表
            table_query = GET_TABLES_BASIC if fast_mode else GET_TABLES_IN_SCHEMA
            tables_result = _safe_query(
                sql_query=table_query.format(schema_name=schema_name),
                database_name=database_name,
                operation_name=f"获取Schema {schema_name} 的表",
                timeout=60 if fast_mode else 90,
                fallback_result=[],
            )

            if not tables_result:
                schema_info.append("  (无表)")
                continue

            # 第四步：获取该Schema的约束信息（在快速模式下跳过）
            constraints_info = (
                {}
                if fast_mode
                else _get_schema_constraints_safe(database_name, schema_name)
            )

            # 遍历每个表
            for table_row in tables_result:
                table_name = table_row.get("TABLE_NAME")
                table_type = table_row.get("TABLE_TYPE", "BASE TABLE")

                if not table_name:
                    continue

                logging.info(f"  处理表: {schema_name}.{table_name}")
                schema_info.append(f"\n  Table: {table_name} ({table_type})")

                # 第三步：获取该表的列信息
                column_query = GET_COLUMNS_BASIC if fast_mode else GET_COLUMNS_FOR_TABLE
                columns_result = _safe_query(
                    sql_query=column_query.format(
                        schema_name=schema_name, table_name=table_name
                    ),
                    database_name=database_name,
                    operation_name=f"获取表 {schema_name}.{table_name} 的列信息",
                    timeout=45 if fast_mode else 75,
                    fallback_result=[],
                )

                if columns_result:
                    schema_info.append("    Columns:")
                    for col_row in columns_result:
                        column_info = _format_column_info(col_row)
                        schema_info.append(f"      {column_info}")

                # 添加约束信息（在快速模式下跳过）
                if not fast_mode:
                    table_constraints = _get_table_constraints_from_result(
                        constraints_info, table_name
                    )
                    if table_constraints:
                        schema_info.append("    Constraints:")
                        for constraint in table_constraints:
                            schema_info.append(f"      {constraint}")

        result = "\n".join(schema_info)
        logging.info(f"Schema提取完成，总长度: {len(result)} 字符")
        return result

    except Exception as e:
        logging.error(f"提取schema信息时发生错误: {e}")
        return f"提取schema信息失败: {str(e)}"


def _format_column_info(col_row: Dict[str, Any]) -> str:
    """
    格式化列信息
    """
    column_name = col_row.get("COLUMN_NAME", "")
    data_type = col_row.get("DATA_TYPE", "")
    is_nullable = col_row.get("IS_NULLABLE", "YES")
    column_default = col_row.get("COLUMN_DEFAULT")
    char_max_length = col_row.get("CHARACTER_MAXIMUM_LENGTH")
    numeric_precision = col_row.get("NUMERIC_PRECISION")
    numeric_scale = col_row.get("NUMERIC_SCALE")

    # 构建数据类型描述
    type_desc = data_type
    if char_max_length:
        type_desc += f"({char_max_length})"
    elif numeric_precision is not None:
        if numeric_scale is not None and numeric_scale > 0:
            type_desc += f"({numeric_precision},{numeric_scale})"
        else:
            type_desc += f"({numeric_precision})"

    # 构建完整描述
    nullable = "NULL" if is_nullable == "YES" else "NOT NULL"
    default = f" DEFAULT {column_default}" if column_default else ""

    return f"{column_name}: {type_desc} {nullable}{default}"


def _get_schema_constraints_safe(
    database_name: str, schema_name: str
) -> Dict[str, List[Dict[str, Any]]]:
    """
    安全获取指定Schema的所有约束信息（带重试和容错）
    """
    constraints_info = {"primary_unique": [], "foreign_keys": []}

    # 获取主键和唯一约束
    pk_unique_result = _safe_query(
        sql_query=GET_CONSTRAINTS_FOR_SCHEMA.format(schema_name=schema_name),
        database_name=database_name,
        operation_name=f"获取Schema {schema_name} 的主键和唯一约束",
        timeout=60,
        fallback_result=[],
    )
    constraints_info["primary_unique"] = pk_unique_result

    # 获取外键约束
    fk_result = _safe_query(
        sql_query=GET_FOREIGN_KEYS_FOR_SCHEMA.format(schema_name=schema_name),
        database_name=database_name,
        operation_name=f"获取Schema {schema_name} 的外键约束",
        timeout=60,
        fallback_result=[],
    )
    constraints_info["foreign_keys"] = fk_result

    return constraints_info


def _get_schema_constraints(
    database_name: str, schema_name: str
) -> Dict[str, List[Dict[str, Any]]]:
    """
    获取指定Schema的所有约束信息（原始版本，保持向后兼容）
    """
    constraints_info = {"primary_unique": [], "foreign_keys": []}

    try:
        # 获取主键和唯一约束
        pk_unique_result = snowflake_sql_query(
            sql_query=GET_CONSTRAINTS_FOR_SCHEMA.format(schema_name=schema_name),
            database_id=database_name,
            timeout=30,
        )
        constraints_info["primary_unique"] = pk_unique_result or []

        # 获取外键约束
        fk_result = snowflake_sql_query(
            sql_query=GET_FOREIGN_KEYS_FOR_SCHEMA.format(schema_name=schema_name),
            database_id=database_name,
            timeout=30,
        )
        constraints_info["foreign_keys"] = fk_result or []

    except Exception as e:
        logging.warning(f"获取Schema {schema_name} 约束信息失败: {e}")

    return constraints_info


def _get_table_constraints_from_result(
    constraints_info: Dict[str, List[Dict[str, Any]]], table_name: str
) -> List[str]:
    """
    从约束信息结果中提取指定表的约束描述
    """
    constraints = []

    # 处理主键和唯一约束
    constraint_groups = {}
    for constraint in constraints_info["primary_unique"]:
        if constraint.get("TABLE_NAME") == table_name:
            constraint_name = constraint.get("CONSTRAINT_NAME")
            constraint_type = constraint.get("CONSTRAINT_TYPE")
            column_name = constraint.get("COLUMN_NAME")

            if constraint_name not in constraint_groups:
                constraint_groups[constraint_name] = {
                    "type": constraint_type,
                    "columns": [],
                }
            constraint_groups[constraint_name]["columns"].append(column_name)

    for constraint_name, info in constraint_groups.items():
        columns_str = ", ".join(info["columns"])
        constraints.append(f"{info['type']}: {columns_str}")

    # 处理外键约束
    for fk in constraints_info["foreign_keys"]:
        if fk.get("TABLE_NAME") == table_name:
            fk_column = fk.get("COLUMN_NAME")
            ref_schema = fk.get("REFERENCED_TABLE_SCHEMA")
            ref_table = fk.get("REFERENCED_TABLE_NAME")
            ref_column = fk.get("REFERENCED_COLUMN_NAME")

            constraints.append(
                f"FOREIGN KEY: {fk_column} -> {ref_schema}.{ref_table}.{ref_column}"
            )

    return constraints


if __name__ == "__main__":
    """
    测试数据库schema提取功能
    """
    # 设置日志级别
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # 测试数据库名称
    test_database_name = "CRYPTO"

    print("=== 测试数据库Schema提取 ===\n")

    try:
        # 测试1: 快速模式
        # print("1. 快速模式测试（基本信息，避免超时）")
        # print("-" * 40)
        # schema_info = extract_db_info(test_database_name, fast_mode=True)
        # print("快速模式结果:")
        # print(schema_info[:1000] + "..." if len(schema_info) > 1000 else schema_info)

        # 测试2: 指定Schema（如果需要）
        # schema_info = extract_db_info(test_database_name, specific_schema="PUBLIC")
        # print("指定Schema结果:")
        # print(schema_info[:1000] + "..." if len(schema_info) > 1000 else schema_info)

        # 测试3: 完整模式（谨慎使用）
        print("3. 完整模式测试（包含约束信息）")
        print("-" * 40)
        schema_info = extract_db_info(test_database_name, fast_mode=False)
        print("完整模式结果:")
        print(schema_info[:1000] + "..." if len(schema_info) > 1000 else schema_info)

        print("✓ 测试完成!")
        # 保存schema信息到文件
        print("\n保存schema信息到文件...")
        try:
            output_file = f"schema_info_{test_database_name}.txt"
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(schema_info)
            print(f"✓ Schema信息已保存到文件: {output_file}")
        except Exception as e:
            print(f"✗ 保存文件失败: {e}")

    except Exception as e:
        print(f"✗ 测试失败: {e}")
