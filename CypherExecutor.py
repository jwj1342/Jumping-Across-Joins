from neo4j import GraphDatabase, TransactionError
import logging

class CypherExecutor:
    def __init__(self, uri, username, password):
        """
        初始化 CypherExecutor，建立与 Neo4j 数据库的连接。

        Args:
            uri (str): Neo4j 数据库的连接 URI (例如: "bolt://localhost:7687")
            username (str): 数据库用户名
            password (str): 数据库密码
        """
        try:
            self._driver = GraphDatabase.driver(uri, auth=(username, password))
            # 验证连接是否成功
            self._driver.verify_connectivity()
            logging.info("成功连接到 Neo4j 数据库。")
        except Exception as e:
            logging.error(f"连接 Neo4j 数据库失败: {e}")
            self._driver = None # 确保如果连接失败，驱动对象为None

    def _execute_cypher_in_transaction(self, tx, cypher_statement, parameters=None):
        """
        在事务中执行单个 Cypher 语句。这是供内部调用的辅助方法。

        Args:
            tx: Neo4j 事务对象。
            cypher_statement (str): 要执行的 Cypher 语句。
            parameters (dict, optional): Cypher 语句的参数。默认为 None。

        Returns:
            neo4j.Result: Cypher 查询的结果对象。
        """
        if parameters is None:
            parameters = {}
        result = tx.run(cypher_statement, parameters)
        logging.info(f"成功执行 Cypher 语句:\n{cypher_statement}")
        return result

    def execute_transactional_cypher(self, cypher_statement, parameters=None):
        """
        将输入的 Cypher 语句包装成一个事务并执行。
        如果执行成功，事务提交；如果失败，事务回滚。

        Args:
            cypher_statement (str): 要执行的 Cypher 语句 (例如: "CREATE (n:TestNode {id: $id})")。
            parameters (dict, optional): Cypher 语句中使用的参数 (例如: {"id": 123})。默认为 None。

        Returns:
            bool: 如果事务成功提交则为 True，否则为 False。
            list: 如果成功，返回查询结果的记录列表；如果失败，返回空列表。
        """
        if not self._driver:
            logging.error("数据库连接未建立，无法执行 Cypher 语句。")
            return False, []

        with self._driver.session() as session:
            try:
                # 使用 write_transaction 包装 Cypher 语句
                # 匿名函数 (lambda) 用于将 cypher_statement 和 parameters 传递给 _execute_cypher_in_transaction
                # _execute_cypher_in_transaction 会接收 tx 对象
                result = session.write_transaction(
                    self._execute_cypher_in_transaction,
                    cypher_statement,
                    parameters
                )
                logging.info("事务成功提交。")
                # 返回所有记录，通常 result.data() 适用于返回节点/关系的属性
                return True, result.data()
            except TransactionError as e:
                logging.error(f"Cypher 语句执行失败，事务已回滚。Neo4j 错误: {e}")  
                return False, []
            except Exception as e:
                logging.error(f"Cypher 语句执行失败，事务已回滚。未知错误: {e}")
                return False, []

    def close(self):
        """
        关闭数据库连接。
        """
        if self._driver:
            self._driver.close()
            logging.info("Neo4j 数据库连接已关闭。")
