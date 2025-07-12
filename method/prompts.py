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
from typing import Literal, List

class FieldExtractionResponse(BaseModel):
    """Field extraction response model"""
    fields: List[str] = Field(
        description="List of potential database fields extracted from the query"
    )

class ErrorAnalysisResponse(BaseModel):
    """错误分析响应模型"""
    error_type: Literal["schema_info_insufficient", "syntax_error", "logic_error", "empty_result"] = Field(
        description="错误类型：schema_info_insufficient(schema信息不足), syntax_error(语法错误), logic_error(逻辑错误), empty_result(执行成功但返回空数据)"
    )
    analysis: str = Field(
        description="错误分析详情"
    )
    suggested_action: Literal["request_more_schema", "fix_sql", "end_process"] = Field(
        description="建议采取的行动：request_more_schema(请求更多schema信息), fix_sql(修复SQL), end_process(结束处理)"
    )
    
class SqlUnderstandingResponse(BaseModel):
    """SQL语义理解响应模型"""
    sql_understanding: str = Field(
        description="对SQL语句的语义理解和解释"
    )
    expected_behavior: str = Field(
        description="SQL语句的预期行为和应该返回的数据类型"
    )

class SqlFixResponse(BaseModel):
    """SQL修复响应模型"""
    fixed_sql: str = Field(
        description="修复后的SQL语句"
    )
    explanation: str = Field(
        description="修复说明"
    )

# Initialize output parsers
field_extraction_parser = JsonOutputParser(pydantic_object=FieldExtractionResponse)
sql_parser = JsonOutputParser(pydantic_object=SqlQueryResponse)
error_analysis_parser = JsonOutputParser(pydantic_object=ErrorAnalysisResponse)
sql_understanding_parser = JsonOutputParser(pydantic_object=SqlUnderstandingResponse)
sql_fix_parser = JsonOutputParser(pydantic_object=SqlFixResponse)

# Field extraction prompt
FIELD_EXTRACTION_PROMPT = PromptTemplate(
    template="""You are a database field extraction expert. Analyze the user query and extract potential field names and related concepts.

User Query: {user_query}
Maximum Fields to Extract: {max_fields}

Your task is to:
1. Extract explicit field names mentioned in the query
2. Identify potential field name variations (e.g., user_id, userId, user_name)
3. Infer related business concepts that might map to field names
4. Consider common field patterns for the identified concepts

Focus on these field types:
- Identifiers (e.g., id, code, key)
- Names and descriptions
- Dates and timestamps
- Status and types
- Amounts and quantities
- Addresses and locations
- Contact information
- Metadata fields

Guidelines:
1. Include both snake_case and camelCase variations
2. Consider common prefixes and suffixes
3. Include related business domain terms
4. Focus on database-style naming conventions
5. **IMPORTANT**: Extract ONLY the top {max_fields} most relevant fields, prioritizing the most important and likely field names first

{format_instructions}

Extract potential fields from the query. Return EXACTLY {max_fields} fields or fewer if no more relevant fields can be identified. Respond only with valid JSON, no additional text.""",
    input_variables=["user_query", "max_fields"],
    partial_variables={"format_instructions": field_extraction_parser.get_format_instructions()}
)

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
- Each field in the `fields` array contains detailed information including:
  * `name`: The field name
  * `type`: The field data type
  * `description`: Detailed description of what the field contains (use this to understand field semantics)
  * `field_id`: Unique identifier for the field
- **Use field descriptions**: When selecting fields, carefully read the `description` to understand what each field represents and ensure you choose the most appropriate fields for the user query.
- Table Reference Format: You MUST construct fully qualified table names using the pattern `<database>.<schema>.<table>`.
- To do this, you must navigate the `schema_info` JSON tree. The database name is at the root. Iterate through the `schemas` array, and for each schema, iterate through its `tables` array to find the correct table and its fields.
- Example: To reference a table, you would use `{database_id}.<schema_name>.<table_name>`. For instance, if `database_id` is 'PROD_DB', a schema is 'SALES', and a table is 'TRANSACTIONS', the full path is `PROD_DB.SALES.TRANSACTIONS`.
- Field Names: Enclose field names in double quotes, e.g., "field_name".
- Use standard SQL syntax.

Important Notes:
1. **Crucial**: Always construct table references using the format `{database_id}.<schema_name>.<table_name>`. The `database_id` variable is provided for convenience and must match the `database` name in the `schema_info` object.
2. **Use field descriptions**: Before selecting fields, read their descriptions to understand their semantic meaning and choose the most relevant fields for the query.
3. All field names must be enclosed in double quotes.

{format_instructions}

Generate the SQL statement. Respond only with valid JSON, no additional text.""",
    input_variables=["user_query", "database_id", "schema_info", "execution_history"],
    partial_variables={"format_instructions": sql_parser.get_format_instructions()}
)

# SQL错误分析提示模板
ERROR_ANALYSIS_PROMPT = PromptTemplate(
    template="""You are a SQL error analysis expert. Analyze the SQL execution result and determine if there are any issues.

User Query: {user_query}
Generated SQL: {generated_sql}
Error Message: {error_message}
Database ID: {database_id}
Schema Information: {schema_info}
Result Data: {result_data}
SQL Understanding: {sql_understanding}

Your task is to analyze the result and determine:
1. What type of issue this is (if any)
2. What caused the issue
3. What action should be taken

Analysis Scenarios:
1. If error_message is not empty: This is a SQL execution error
2. If error_message is empty but result_data is empty or null: This is an empty result scenario
3. If both error_message is empty and result_data is not empty: This is a successful execution (should not reach analysis)

Error Types:
- schema_info_insufficient: The issue is caused by missing or incomplete schema information
- syntax_error: The issue is caused by SQL syntax errors (only when there's an actual error_message)
- logic_error: The issue is caused by logical mistakes in the query (both execution errors and empty results)
- empty_result: The SQL executed successfully but returned no data (when error_message is empty but result_data is empty)

For empty_result cases, consider:
- Does the SQL logic match the user's intent?
- Are the filtering conditions too restrictive?
- Are the table joins correct?
- Are the field references accurate?
- Is the data actually available in the database for this query?

Suggested Actions:
- request_more_schema: Request more detailed schema information from InfoAgent
- fix_sql: Attempt to fix the SQL statement directly
- end_process: The issue cannot be resolved, end the process

{format_instructions}

Analyze the result and provide your assessment. Respond only with valid JSON, no additional text.""",
    input_variables=["user_query", "generated_sql", "error_message", "database_id", "schema_info", "result_data", "sql_understanding"],
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
- The schema information contains detailed field descriptions that help understand field semantics
- Each field includes: name, type, description (which explains what the field contains), and field_id
- **Use field descriptions**: When fixing SQL, carefully read field descriptions to ensure correct field selection and usage
- Table Reference Format: {database_id}.<schema_name>.<table_name>
- Field Names: Enclose field names in double quotes, e.g., "field_name"
- Use standard SQL syntax

{format_instructions}

Fix the SQL statement and provide explanation. Respond only with valid JSON, no additional text.""",
    input_variables=["user_query", "original_sql", "error_message", "error_analysis", "database_id", "schema_info", "conversation_history"],
    partial_variables={"format_instructions": sql_fix_parser.get_format_instructions()}
)

# SQL语义理解提示模板
SQL_UNDERSTANDING_PROMPT = PromptTemplate(
    template="""You are a SQL semantic analysis expert. Analyze the given SQL statement and provide a detailed understanding of what it does.

User Query: {user_query}
Generated SQL: {sql_query}
Database ID: {database_id}
Schema Information: {schema_info}

Your task is to:
1. Provide a clear explanation of what the SQL statement does
2. Describe what kind of data it should return based on the user query
3. Explain the logic and joins used in the SQL
4. Identify what business question the SQL is trying to answer

Focus on:
- The tables being queried and their relationships
- The fields being selected and their meaning
- Any filtering conditions and their purpose
- Expected result set characteristics (number of rows, data types, etc.)
- How well the SQL aligns with the user's intent

{format_instructions}

Analyze the SQL statement and provide your understanding. Respond only with valid JSON, no additional text.""",
    input_variables=["user_query", "sql_query", "database_id", "schema_info"],
    partial_variables={"format_instructions": sql_understanding_parser.get_format_instructions()}
)