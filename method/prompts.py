"""
Prompt templates for InfoAgent and SqlAgent using LangChain PromptTemplate.
All prompts are in English following LangChain best practices.
"""

from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from Communicate import (
    SqlQueryResponse
)

# 添加错误分析响应模型
from pydantic import BaseModel, Field
from typing import Literal

class ErrorAnalysisResponse(BaseModel):
    """错误分析响应模型"""
    error_type: Literal["schema_info_insufficient", "syntax_error", "logic_error"] = Field(
        description="错误类型：schema_info_insufficient(schema信息不足), syntax_error(语法错误), logic_error(逻辑错误)"
    )
    analysis: str = Field(
        description="错误分析详情"
    )
    suggested_action: Literal["request_more_schema", "fix_sql", "end_process"] = Field(
        description="建议采取的行动：request_more_schema(请求更多schema信息), fix_sql(修复SQL), end_process(结束处理)"
    )
    
class SqlFixResponse(BaseModel):
    """SQL修复响应模型"""
    fixed_sql: str = Field(
        description="修复后的SQL语句"
    )
    explanation: str = Field(
        description="修复说明"
    )
    confidence: int = Field(
        description="修复信心度(1-10)",
        ge=1, le=10
    )

# Initialize output parsers
sql_parser = JsonOutputParser(pydantic_object=SqlQueryResponse)
error_analysis_parser = JsonOutputParser(pydantic_object=ErrorAnalysisResponse)
sql_fix_parser = JsonOutputParser(pydantic_object=SqlFixResponse)

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

# SQL错误分析提示模板
ERROR_ANALYSIS_PROMPT = PromptTemplate(
    template="""You are a SQL error analysis expert. Analyze the SQL execution error and determine the root cause.

User Query: {user_query}
Generated SQL: {generated_sql}
Error Message: {error_message}
Database ID: {database_id}
Schema Information: {schema_info}

Your task is to analyze the error and determine:
1. What type of error this is
2. What caused the error
3. What action should be taken

Error Types:
- schema_info_insufficient: The error is caused by missing or incomplete schema information
- syntax_error: The error is caused by SQL syntax issues
- logic_error: The error is caused by logical mistakes in the query

Suggested Actions:
- request_more_schema: Request more detailed schema information from InfoAgent
- fix_sql: Attempt to fix the SQL statement directly
- end_process: The error cannot be resolved, end the process

{format_instructions}

Analyze the error and provide your assessment. Respond only with valid JSON, no additional text.""",
    input_variables=["user_query", "generated_sql", "error_message", "database_id", "schema_info"],
    partial_variables={"format_instructions": error_analysis_parser.get_format_instructions()}
)

# SQL修复提示模板
SQL_FIX_PROMPT = PromptTemplate(
    template="""You are a SQL repair expert. Based on the error analysis, fix the problematic SQL statement.

User Query: {user_query}
Original SQL: {original_sql}
Error Message: {error_message}
Error Analysis: {error_analysis}
Database ID: {database_id}
Schema Information: {schema_info}
Conversation History: {conversation_history}

Your task is to generate a corrected SQL statement that:
1. Addresses the identified error
2. Maintains the original intent of the user query
3. Uses correct syntax and schema references

Database Specifications:
- Database Type: Snowflake
- Table Reference Format: {database_id}.<schema_name>.<table_name>
- Field Names: Enclose field names in double quotes, e.g., "field_name"
- Use standard SQL syntax

{format_instructions}

Fix the SQL statement and provide explanation. Respond only with valid JSON, no additional text.""",
    input_variables=["user_query", "original_sql", "error_message", "error_analysis", "database_id", "schema_info", "conversation_history"],
    partial_variables={"format_instructions": sql_fix_parser.get_format_instructions()}
)