import re
import os
import logging
from typing import List, Dict, Any, Tuple, Optional
from CypherExecutor import CypherExecutor
import dotenv
from openai import OpenAI
from prompts import get_cypher_generation_prompt, get_neo4j_rules
from ExtraDbInfo import extract_db_info

# 定义SQL结果的类型
SqlResult = List[Dict[str, Any]]

# 定义隐藏关系对的类型
HiddenRelationPair = Tuple[str, str]


def schema2cypher(
    schema_info: str,
    hidden_relations: Optional[List[HiddenRelationPair]] = None,
) -> str:
    """
    将schema信息转换为Neo4j的Cypher查询

    Args:
        schema_info: 数据库schema信息字符串
        hidden_relations: 隐藏关系对列表，每个元素是(table1, table2)的元组

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

    # 获取建模规则
    rules = get_neo4j_rules()

    # 初始化LLM
    if api_key and base_url:
        llm = OpenAI(api_key=api_key, base_url=base_url)
        logging.info("LLM initialized")
    else:
        logging.warning("LLM not initialized - API key or base URL missing")
        llm = None

    # 生成基础的Cypher查询
    base_cypher = generate_base_cypher(schema_info, rules, llm)

    executor = CypherExecutor()
    try:
        result = executor.execute_transactional_cypher(base_cypher)
        logging.info(f"Cypher query executed successfully: {result}")
    except Exception as e:
        logging.error(f"Error executing Cypher query: {e}")
        raise  # 重新抛出异常，包括连续失败异常
    finally:
        executor.close()

    return base_cypher


def generate_base_cypher(schema_info: str, rules: str, llm: OpenAI) -> str:
    """
    生成基础的Cypher查询
    """
    if not llm:
        logging.error("LLM not available for generating Cypher queries")
        return ""

    # 使用prompts模块中的prompt模板
    prompt = get_cypher_generation_prompt(schema_info, rules)

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
    这是端到端测试代码，测试将数据库schema转换为图结构
    """
    # 测试数据库名称
    test_database_name = "DEATH"

    # 测试Schema提取
    print("=== 测试Schema提取 ===")
    schema_info = extract_db_info(test_database_name)
    print("提取的Schema信息：")
    print(schema_info)

    # 测试完整的转换过程（需要在.env文件中配置API密钥）
    print("\n=== 测试Schema到Cypher转换 ===")
    result = schema2cypher(schema_info)

    print("生成的Cypher查询：")
    print(result if result else "未生成查询（需要配置LLM）")

    print("\n=== 测试完成 ===")
