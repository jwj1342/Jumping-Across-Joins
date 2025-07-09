"""
Prompt templates for InfoAgent and SqlAgent using LangChain PromptTemplate.
All prompts are in English following LangChain best practices.
"""

from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from method.Communicate import (
    SqlQueryResponse
)

# Initialize output parsers
sql_parser = JsonOutputParser(pydantic_object=SqlQueryResponse)

# SQL query generation prompt
SQL_AGENT_PROMPT = PromptTemplate(
template="""You are a professional SQL generation agent. Generate accurate SQL statements based on user queries and database schema information.

User Query: {user_query}
Database ID: {database_id}
Schema Information: {schema_info}
Execution History: {execution_history}

Your responsibilities:
1. Analyze the natural language query
2. Generate SQL statements based on the provided schema information
3. Ensure SQL syntax is correct and follows Snowflake specifications
4. Verify the generated SQL through self-checking

Database Specifications:
- Database Type: Snowflake
- The schema of the database is provided in `schema_info` as a JSON object with a tree structure: `database` -> `schemas` -> `tables` -> `fields`.
- Table Reference Format: You MUST construct fully qualified table names using the pattern `<database>.<schema>.<table>`.
- To do this, you must navigate the `schema_info` JSON tree. The database name is at the root. Iterate through the `schemas` array, and for each schema, iterate through its `tables` array to find the correct table and its fields.
- Example: To reference a table, you would use `{database_id}.<schema_name>.<table_name>`. For instance, if `database_id` is 'PROD_DB', a schema is 'SALES', and a table is 'TRANSACTIONS', the full path is `PROD_DB.SALES.TRANSACTIONS`.
- Field Names: Enclose field names in double quotes, e.g., "field_name".
- Use standard SQL syntax.

Important Notes:
1. **Crucial**: Always construct table references using the format `{database_id}.<schema_name>.<table_name>`. The `database_id` variable is provided for convenience and must match the `database` name in the `schema_info` object.
2. All field names must be enclosed in double quotes.

{format_instructions}

Generate the SQL statement. Respond only with valid JSON, no additional text.""",
    input_variables=["user_query", "database_id", "schema_info", "execution_history"],
    partial_variables={"format_instructions": sql_parser.get_format_instructions()}
)