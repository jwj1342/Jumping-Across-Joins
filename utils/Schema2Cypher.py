import re
import os
import logging
from typing import List, Dict, Any, Tuple, Optional
from CypherExecutor import CypherExecutor
import dotenv
from openai import OpenAI

# 定义SQL结果的类型
SqlResult = List[Dict[str, Any]]

# 定义隐藏关系对的类型
HiddenRelationPair = Tuple[str, str]


def schema2cypher(
    sql_result: SqlResult,
    hidden_relations: Optional[List[HiddenRelationPair]] = None,
    rules_file: str = "utils/neo4j_rules.md",
) -> str:
    """
    将SQL查询结果转换为Neo4j的Cypher查询

    Args:
        sql_result: SELECT * FROM INFORMATION_SCHEMA.COLUMNS 的查询结果
        hidden_relations: 隐藏关系对列表，每个元素是(table1, table2)的元组
        rules_file: 规则文件路径

    Returns:
        生成的Cypher查询字符串
    """

    # 初始化日志
    logging.basicConfig(level=logging.INFO)

    # 加载环境变量
    dotenv.load_dotenv(".env")

    # 从环境变量读取配置
    api_key = os.getenv("OPENAI_API_KEY")
    base_url = os.getenv("BASE_URL")

    # 提取schema信息
    schema_info = extract_schema_from_sql_result(sql_result)

    # 读取规则文件
    with open(rules_file, "r", encoding="utf-8") as f:
        rules = f.read()

    # 初始化LLM
    if api_key and base_url:
        llm = OpenAI(api_key=api_key, base_url=base_url)
        logging.info("LLM initialized")
    else:
        logging.warning("LLM not initialized - API key or base URL missing")
        llm = None

    # 生成基础的Cypher查询
    base_cypher = generate_base_cypher(schema_info, rules, llm)

    # 如果有隐藏关系，生成隐藏关系的Cypher查询
    if hidden_relations and llm:
        hidden_cypher = generate_hidden_relations_cypher(hidden_relations, rules, llm)
        final_cypher = combine_cypher_queries(base_cypher, hidden_cypher)
    else:
        final_cypher = base_cypher

    executor = CypherExecutor()
    try:
        # 使用新的多语句执行方法
        result = executor.execute_transactional_cypher(final_cypher)
        logging.info(f"Cypher query executed successfully: {result}")
    except Exception as e:
        logging.error(f"Error executing Cypher query: {e}")
        raise  # 重新抛出异常，包括连续失败异常
    finally:
        executor.close()

    return final_cypher


def extract_schema_from_sql_result(sql_result: SqlResult) -> str:
    """
    从SQL查询结果中提取schema信息
    """
    schema_info = []

    # 按表名分组
    tables = {}
    for row in sql_result:
        table_name = row.get("TABLE_NAME")
        column_name = row.get("COLUMN_NAME")
        data_type = row.get("DATA_TYPE")
        is_nullable = row.get("IS_NULLABLE", "YES")
        column_default = row.get("COLUMN_DEFAULT")

        if table_name not in tables:
            tables[table_name] = []

        tables[table_name].append(
            {
                "column_name": column_name,
                "data_type": data_type,
                "is_nullable": is_nullable,
                "column_default": column_default,
            }
        )

    # 构建schema描述
    for table_name, columns in tables.items():
        schema_info.append(f"Table: {table_name}")
        for col in columns:
            nullable = "NULL" if col["is_nullable"] == "YES" else "NOT NULL"
            default = (
                f" DEFAULT {col['column_default']}" if col["column_default"] else ""
            )
            schema_info.append(
                f"  - {col['column_name']}: {col['data_type']} {nullable}{default}"
            )
        schema_info.append("")

    return "\n".join(schema_info)


def generate_base_cypher(schema_info: str, rules: str, llm: OpenAI) -> str:
    """
    生成基础的Cypher查询
    """
    if not llm:
        logging.error("LLM not available for generating Cypher queries")
        return ""

    prompt = f"""
    You are a helpful assistant that generates Cypher queries based on the given schema and rules.
    The schema describes the structure of the table database, which we need to transform into a graph database.
    The rules contain details about the representation of the schema in the graph database.
    Including the node labels, relationship types and how to represent the relationships in the graph database.

    Here is the schema: {schema_info}
    Here are the rules: {rules}

    Please generate the Cypher queries to create a comprehensive graph database schema based on the given schema and rules.
    
    IMPORTANT: Return a valid JSON object with a single "cypher" field containing ALL the Cypher statements concatenated with semicolons and newlines.
    Do NOT create multiple "cypher" fields. Use a single field with all statements.
    
    Example response format:
    {{"cypher": "CREATE (table1:Table {{name: 'table1'}});\nCREATE (col1:Column {{name: 'col1'}});\nCREATE (table1)-[:Contains]->(col1);"}}
    """

    try:
        response = llm.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
        )
        raw_response = response.choices[0].message.content.strip()
        logging.info(f"Raw response: {raw_response}")

        # 解析JSON响应
        import json

        try:
            response_data = json.loads(raw_response)
            cypher_query = response_data.get("cypher", "")

            if cypher_query:
                logging.info(
                    f"Successfully parsed JSON response, cypher length: {len(cypher_query)}"
                )
                return cypher_query
            else:
                logging.warning("JSON response parsed but no 'cypher' field found")

        except json.JSONDecodeError as json_error:
            logging.error(f"JSON parsing failed: {json_error}")
            logging.info("Attempting to extract cypher from markdown format")

        # 如果JSON解析失败，尝试从markdown中提取
        cypher_query = extract_cypher_from_markdown(raw_response)
        if cypher_query:
            logging.info(
                f"Successfully extracted cypher from markdown, length: {len(cypher_query)}"
            )
            return cypher_query
        else:
            logging.error(
                "Failed to extract cypher from both JSON and markdown formats"
            )
            return ""

    except Exception as e:
        logging.error(f"Error generating base Cypher query: {e}")
        return ""


def generate_hidden_relations_cypher(
    hidden_relations: List[HiddenRelationPair], rules: str, llm: OpenAI
) -> str:
    """
    生成隐藏关系的Cypher查询
    """
    if not llm:
        logging.error("LLM not available for generating hidden relations Cypher")
        return ""

    hidden_relations_desc = []
    for table1, table2 in hidden_relations:
        hidden_relations_desc.append(f"Hidden relation between {table1} and {table2}")

    hidden_relations_text = "\n".join(hidden_relations_desc)

    prompt = f"""
    You need to generate Cypher queries to create hidden relationships between tables.
    These are implicit relationships that are not represented by foreign keys but exist conceptually.
    
    Hidden relationships to create:
    {hidden_relations_text}
    
    Rules for hidden relationships:
    {rules}
    
    Please generate Cypher queries to create HiddenRelation relationships between the specified tables.
    The HiddenRelation should be bidirectional (create relationships in both directions).
    
    IMPORTANT: Return a valid JSON object with a single "cypher" field containing ALL the Cypher statements.
    Do NOT create multiple "cypher" fields. Use semicolons to separate multiple statements.
    
    Example response format:
    {{"cypher": "MATCH (t1:Table {{name: 'table1'}}), (t2:Table {{name: 'table2'}}) CREATE (t1)-[:HiddenRelation]->(t2), (t2)-[:HiddenRelation]->(t1);"}}
    """

    try:
        response = llm.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
        )
        raw_response = response.choices[0].message.content.strip()
        logging.info(f"Hidden relations raw response: {raw_response}")

        # 解析JSON响应
        import json

        try:
            response_data = json.loads(raw_response)
            cypher_query = response_data.get("cypher", "")

            if cypher_query:
                logging.info(
                    f"Successfully parsed hidden relations JSON response, cypher length: {len(cypher_query)}"
                )
                return cypher_query
            else:
                logging.warning(
                    "Hidden relations JSON response parsed but no 'cypher' field found"
                )

        except json.JSONDecodeError as json_error:
            logging.error(f"Hidden relations JSON parsing failed: {json_error}")
            logging.info("Attempting to extract cypher from markdown format")

        # 如果JSON解析失败，尝试从markdown中提取
        cypher_query = extract_cypher_from_markdown(raw_response)
        if cypher_query:
            logging.info(
                f"Successfully extracted hidden relations cypher from markdown, length: {len(cypher_query)}"
            )
            return cypher_query
        else:
            logging.error(
                "Failed to extract hidden relations cypher from both JSON and markdown formats"
            )
            return ""

    except Exception as e:
        logging.error(f"Error generating hidden relations Cypher query: {e}")
        return ""


def combine_cypher_queries(base_cypher: str, hidden_cypher: str) -> str:
    """
    合并基础Cypher查询和隐藏关系Cypher查询
    """
    if not base_cypher:
        return hidden_cypher
    if not hidden_cypher:
        return base_cypher

    return f"{base_cypher}\n\n// Hidden Relations\n{hidden_cypher}"


def extract_cypher_from_markdown(text: str) -> str:
    """
    从可能包含markdown代码块的文本中提取Cypher查询
    """
    try:
        # 首先尝试查找 ```cypher ... ``` 格式的代码块
        cypher_pattern = r"```cypher\s*(.*?)\s*```"
        match = re.search(cypher_pattern, text, re.DOTALL)
        if match:
            cypher_query = match.group(1).strip()
            logging.info(f"Cypher query extracted successfully: {cypher_query}")
            return cypher_query

        # 如果没有找到cypher标记的代码块，尝试查找普通的 ``` ... ``` 代码块
        code_pattern = r"```\s*(.*?)\s*```"
        match = re.search(code_pattern, text, re.DOTALL)
        if match:
            cypher_query = match.group(1).strip()
            logging.info(
                f"Cypher query block not found, but found a normal code block: {cypher_query}"
            )
            return cypher_query

    except Exception as e:
        logging.error(f"Error extracting cypher query from markdown: {e}")
        return ""

    logging.info("No cypher query found in the response")
    return ""


if __name__ == "__main__":
    """
    这是测试这个文件是否正常运行的测试代码
    """
    from SnowConnect import snowflake_sql_query

    test_sql_result = snowflake_sql_query(
        sql_query="SELECT * FROM INFORMATION_SCHEMA.COLUMNS",
        database_id="CRYPTO",
        timeout=10,
    )
    print(test_sql_result)

    # 定义隐藏关系
    test_hidden_relations = [
        ("customers", "products"),  # 隐藏关系
        ("orders", "products"),
    ]

    # 测试不使用LLM的情况（只提取schema）
    print("=== 测试Schema提取 ===")
    schema_info = extract_schema_from_sql_result(test_sql_result)
    print("提取的Schema信息：")
    print(schema_info)

    # 测试函数调用（需要在.env文件中配置API密钥）
    print("\n=== 测试函数调用 ===")
    result = schema2cypher(sql_result=test_sql_result, hidden_relations=None)

    print("生成的Cypher查询：")
    print(result if result else "未生成查询（需要配置LLM）")

    print("\n=== 测试完成 ===")
