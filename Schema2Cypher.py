import re
import os
import logging
from CypherExecutor import CypherExecutor
import dotenv
from openai import OpenAI

dotenv.load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_URL = os.getenv("OPENAI_URL")


class Schema2Cypher:
    def __init__(self, schema_file, rules_file):
        self.schema_file = schema_file
        self.rules_file = rules_file
        self.schema = None
        self.rules = None
        self.demands = None
        self.llm = None
        self.cypher_executor = None
        
    #get basic parts
    def get_demands(self,demands):
        self.demands = demands
        logging.info("Demands initialized")

    def get_llm(self,api_key,base_url):
        self.llm = OpenAI(api_key=api_key, base_url=base_url)
        logging.info("LLM initialized")

    def create_cypher_executor(self,uri,username,password):
        self.cypher_executor = CypherExecutor(uri,username,password)
        logging.info("Cypher executor initialized")

    #get schema and rules
    def extract_schema(self,schema_file):
        with open(schema_file, 'r') as f:
            self.schema = f.read()
        logging.info("Schema extracted")
        
    def extract_rules(self,rules_file):
        with open(rules_file, 'r') as f:
            self.rules = f.read()
        logging.info("Rules extracted")

    #change schema and rules
    def change_schema_file(self,new_schema_file):
        if os.path.exists(new_schema_file):
            try:
                self.schema_file = new_schema_file
                logging.info(f"Schema file changed to {new_schema_file}")
            except Exception as e:
                logging.error(f"Error changing schema file: {e}")
                raise e
        else:
            logging.error(f"Schema file {new_schema_file} does not exist")
            raise FileNotFoundError(f"Schema file {new_schema_file} does not exist")
        
    def change_rules_file(self,new_rules_file):
        if os.path.exists(new_rules_file):
            try:
                self.rules_file = new_rules_file
                logging.info(f"Rules file changed to {new_rules_file}")
            except Exception as e:
                logging.error(f"Error changing rules file: {e}")
                raise e
        else:
            logging.error(f"Rules file {new_rules_file} does not exist")
            raise FileNotFoundError(f"Rules file {new_rules_file} does not exist")
    
    #generate cypher
    def generate_cypher(self):
        prompt = f"""
        You are a helpful assistant that generates Cypher queries based on the given schema, rules and demands.
        The schema describes the structure of the table database, which we need to transform into a graph database.
        The rules contain details about the representation of the schema in the graph database.\
        Including the node labels, relationship types and how to represent the relationships in the graph database.
        The demands are the requirements for the Cypher queries.

        Here are my demands:{self.demands}
        Here is the schema:{self.schema}
        Here are the rules:{self.rules}

        The Cypher queries should be in the following format:
        ```cypher
        cypher_query
        ```
        Please generate the Cypher queries based on the given schema, rules and demands.
        """
        response = self.llm.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}
        )
        raw_response = response.choices[0].message.content.strip()
        logging.info(f"Raw response: {raw_response}")
        
        # 使用新的解析方法提取Cypher查询
        cypher_query = self.extract_cypher_from_markdown(raw_response)
        return cypher_query

    def extract_cypher_from_markdown(self, text):
        """
        从可能包含markdown代码块的文本中提取Cypher查询
        """
        try:
            # 首先尝试查找 ```cypher ... ``` 格式的代码块
            cypher_pattern = r'```cypher\s*(.*?)\s*```'
            match = re.search(cypher_pattern, text, re.DOTALL)
            if match:
              #移除markdown代码块标记
                cypher_query = match.group(1).strip()
                cypher_query = cypher_query.replace("```cypher", "").replace("```", "")
                logging.info(f"Cypher query extracted successfully: {cypher_query}")
                return cypher_query
        
        
            # 如果没有找到cypher标记的代码块，尝试查找普通的 ``` ... ``` 代码块
            code_pattern = r'```\s*(.*?)\s*```'
            match = re.search(code_pattern, text, re.DOTALL)
            if match:
                cypher_query = match.group(1).strip()
                cypher_query = cypher_query.replace("```", "")
                logging.info(f"Cypher query block not found, but found a normal code block: {cypher_query}")
                return cypher_query
        
        except Exception as e:
            logging.error(f"Error extracting cypher query from markdown: {e}")
            return None
        
        logging.info(f"No cypher query found in the response")
        return None

    
    def cypher_verify(self):
        pass

    #execute cypher
    def cypher_execute(self,cypher_query):
        if self.cypher_executor is not None:
            result = self.cypher_executor.execute_transactional_cypher(cypher_query)
            logging.info(f"Cypher query executed successfully: {result}")
            return result
        else:
            logging.error("Cypher executor is not initialized")
            return None
        
    def close_cypher_executor(self):
        if self.cypher_executor is not None:
            self.cypher_executor.close()
            logging.info("Cypher executor closed")
