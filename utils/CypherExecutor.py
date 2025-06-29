import os
from dotenv import load_dotenv
import dotenv
from neo4j import GraphDatabase
from neo4j.exceptions import TransientError, ClientError, DatabaseError
import logging


class CypherExecutor:
    def __init__(self):
        """
        初始化 CypherExecutor，建立与 Neo4j 数据库的连接。

        Args:
            uri (str, optional): Neo4j 数据库的连接 URI。如果为None，则从环境变量NEO4J_URI读取
            username (str, optional): 数据库用户名。如果为None，则从环境变量NEO4J_USER读取
            password (str, optional): 数据库密码。如果为None，则从环境变量NEO4J_PASSWORD读取
        """
        # TODO: 从环境变量中获取配置
        # load_dotenv(".env")
        self.uri = "neo4j://10.21.37.13:7687"
        self.username = "neo4j"
        self.password = "neo4j1342"

        try:
            self._driver = GraphDatabase.driver(
                self.uri, auth=(self.username, self.password)
            )
            # 移除立即验证，改为懒加载
            logging.info("Neo4j 驱动已初始化，等待首次使用时验证连接。")
        except Exception as e:
            logging.error(f"连接 Neo4j 数据库失败: {e}")
            self._driver = None  # 确保如果连接失败，驱动对象为None

    def verify_connectivity(self):
        """
        验证与 Neo4j 数据库的连接。

        Returns:
            bool: 连接成功返回 True，失败返回 False
        """
        if not self._driver:
            logging.error("数据库驱动未初始化，无法验证连接。")
            return False

        try:
            self._driver.verify_connectivity()
            logging.info("Neo4j 数据库连接验证成功。")
            return True
        except Exception as e:
            logging.error(f"Neo4j 数据库连接验证失败: {e}")
            return False

    def _execute_multiple_cypher_in_transaction(
        self, tx, cypher_statements_text, parameters=None
    ):
        """
        在单个事务中执行多个 Cypher 语句。这是供内部调用的辅助方法。

        Args:
            tx: Neo4j 事务对象。
            cypher_statements_text (str): 包含多个用分号分隔的Cypher语句的文本
            parameters (dict, optional): Cypher 语句的参数。默认为 None。

        Returns:
            list: 所有Cypher查询的结果数据列表。

        Raises:
            Exception: 当连续3个语句执行失败时抛出异常
        """
        if parameters is None:
            parameters = {}

        # 分割语句
        statements = [
            stmt.strip() for stmt in cypher_statements_text.split(";") if stmt.strip()
        ]

        if not statements:
            logging.warning("没有找到有效的Cypher语句")
            return []

        logging.info(f"在事务中准备执行 {len(statements)} 个Cypher语句")

        all_results = []
        success_count = 0
        consecutive_failures = 0  # 连续失败计数器
        max_consecutive_failures = 3  # 最大连续失败次数

        for i, statement in enumerate(statements, 1):
            # 跳过注释行
            if statement.strip().startswith("//"):
                logging.info(f"跳过注释语句 {i}: {statement[:50]}...")
                continue

            logging.info(f"执行语句 {i}/{len(statements)}: {statement[:100]}...")

            try:
                result = tx.run(statement, parameters)
                # 在事务内部立即处理结果，避免事务关闭后访问
                result_data = result.data()
                all_results.extend(result_data)
                success_count += 1
                consecutive_failures = 0  # 重置连续失败计数器
                logging.info(f"语句 {i} 执行成功")

            except Exception as e:
                consecutive_failures += 1
                logging.error(f"语句 {i} 执行失败: {statement}")
                logging.error(f"错误信息: {e}")
                logging.error(
                    f"连续失败次数: {consecutive_failures}/{max_consecutive_failures}"
                )

                # 检查是否达到连续失败上限
                if consecutive_failures >= max_consecutive_failures:
                    error_msg = f"连续 {consecutive_failures} 个Cypher语句执行失败，终止执行。最后失败的语句: {statement}"
                    logging.error(error_msg)
                    raise Exception(error_msg)

                # 如果没有达到上限，继续执行下一个语句
                continue

        total_executed = len([s for s in statements if not s.strip().startswith("//")])
        logging.info(f"事务中执行完成: {success_count}/{total_executed} 个语句成功")

        return all_results

    def execute_transactional_cypher(self, cypher_statement, parameters=None):
        """
        将输入的 Cypher 语句包装成一个事务并执行。
        支持单个语句或多个用分号分隔的语句。
        如果执行成功，事务提交；如果失败，事务回滚。

        Args:
            cypher_statement (str): 要执行的 Cypher 语句，可以是单个语句或多个用分号分隔的语句
            parameters (dict, optional): Cypher 语句中使用的参数。默认为 None。

        Returns:
            bool: 如果事务成功提交则为 True，否则为 False。
            list: 如果成功，返回查询结果的记录列表；如果失败，返回空列表。

        Raises:
            Exception: 当连续3个语句执行失败时抛出异常
        """
        if not self._driver:
            logging.error("数据库连接未建立，无法执行 Cypher 语句。")
            return False, []

        with self._driver.session() as session:
            try:
                # 多个语句，在一个事务中执行
                result = session.execute_write(
                    self._execute_multiple_cypher_in_transaction,
                    cypher_statement,
                )
                logging.info("事务成功提交。")
                return True, result

            except (TransientError, ClientError, DatabaseError) as e:
                logging.error(f"Cypher 语句执行失败，事务已回滚。Neo4j 错误: {e}")
                return False, []
            except Exception as e:
                logging.error(f"Cypher 语句执行失败，事务已回滚。错误: {e}")
                # 重新抛出连续失败异常
                if "连续" in str(e) and "执行失败" in str(e):
                    raise
                return False, []

    def close(self):
        """
        关闭数据库连接。
        """
        if self._driver:
            self._driver.close()
            logging.info("Neo4j 数据库连接已关闭。")


if __name__ == "__main__":
    """
    这是测试这个文件是否正常运行的测试代码
    """
    executor = CypherExecutor()
    print(executor.verify_connectivity())
    str = """
CREATE (:Person {name: 'Alice', age: 30});
CREATE (:Person {name: 'Bob', age: 25});
CREATE (:Person {name: 'Charlie', age: 35});
CREATE (:City {name: 'London'});
"""
    executor.execute_transactional_cypher(str)
    executor.close()
