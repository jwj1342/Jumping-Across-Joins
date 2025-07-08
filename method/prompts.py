"""
Prompt templates for InfoAgent and SqlAgent using LangChain PromptTemplate.
All prompts are in English following LangChain best practices.
"""

from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from method.Communicate import (
    UsefulTablesResponse,
    UsefulFieldsResponse,
    SqlQueryResponse,
    ErrorAnalysisResponse,
    SchemaInfoResponse
)

# Initialize output parsers
tables_parser = JsonOutputParser(pydantic_object=UsefulTablesResponse)
fields_parser = JsonOutputParser(pydantic_object=UsefulFieldsResponse)
sql_parser = JsonOutputParser(pydantic_object=SqlQueryResponse)
error_parser = JsonOutputParser(pydantic_object=ErrorAnalysisResponse)
schema_parser = JsonOutputParser(pydantic_object=SchemaInfoResponse)

# Table usefulness analysis prompt
TABLE_USEFULNESS_PROMPT = PromptTemplate(
    template="""You are a database table analysis expert. Analyze which tables are useful for answering the user query.

User Query: {user_query}

Available Tables:
{all_tables}

Your task:
1. Analyze the user query's intent and requirements
2. Determine which tables are likely relevant to the query
3. Filter out tables that are clearly unrelated

{format_instructions}

Respond only with valid JSON, no additional text.""",
    input_variables=["user_query", "all_tables"],
    partial_variables={"format_instructions": tables_parser.get_format_instructions()}
)

# Field usefulness analysis prompt
FIELD_USEFULNESS_PROMPT = PromptTemplate(
template="""You are a database field analysis expert. Analyze which fields in the specified table are useful for answering the user query.

User Query: {user_query}
Target Table: {table_name}

All Fields in Table:
{all_fields}

Your task:
1. Analyze what information the user query needs
2. Determine which fields could help answer the query
3. Prioritize key fields (IDs, names, timestamps, etc.)
4. Filter out clearly irrelevant fields

{format_instructions}

Respond only with valid JSON, no additional text.""",
    input_variables=["user_query", "table_name", "all_fields"],
    partial_variables={"format_instructions": fields_parser.get_format_instructions()}
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
- Table Name Format: Use full_table_name from schema information
- Field Names: Enclose field names in double quotes, e.g., "field_name"
- Use standard SQL syntax
- Pay attention to data types and constraints

Important Notes:
1. Use full_table_name from schema information for table references
2. All field names must be enclosed in double quotes, e.g., "field_name"
3. Ensure correct database, schema, and table reference format

{format_instructions}

Generate the SQL statement. Respond only with valid JSON, no additional text.""",
    input_variables=["user_query", "database_id", "schema_info", "execution_history"],
    partial_variables={"format_instructions": sql_parser.get_format_instructions()}
)

# Error analysis prompt
ERROR_ANALYSIS_PROMPT = PromptTemplate(
    template="""You are an SQL error analysis expert. Analyze the following SQL execution error and provide solutions.

SQL Statement: {sql_query}
Error Message: {error_message}
Database ID: {database_id}

Analysis Requirements:
1. Determine the error type (syntax error, table not found, field not found, etc.)
2. Analyze the root cause of the error
3. Provide specific fix suggestions
4. Identify missing schema information if needed

{format_instructions}

Respond only with valid JSON, no additional text.""",
    input_variables=["sql_query", "error_message", "database_id"],
    partial_variables={"format_instructions": error_parser.get_format_instructions()}
)

# Schema information gathering prompt
INFO_AGENT_PROMPT = PromptTemplate(
template="""You are a professional database schema information exploration agent. Your goal is to explore and collect relevant table structure information from the graph database based on user queries and error information.

Current Task:
User Query: {user_query}
Database ID: {database_id}
Known Information: {known_info}
SQL Error Information: {sql_error}

Your responsibilities:
1. Analyze required tables and fields based on user query
2. Search for relevant schema information from graph database
3. If there are SQL errors, analyze error causes and supplement missing schema information
4. Organize information into high-entropy text descriptions

Available Tools:
- get_all_tables(): Get all table information
- get_table_fields(table_name): Get field information for specified table
- find_tables_by_field(field_name): Find related tables by field name
- find_similar_names(name): Find similar table or field names

{format_instructions}

Respond only with valid JSON, no additional text.""",
    input_variables=["user_query", "database_id", "known_info", "sql_error"],
    partial_variables={"format_instructions": schema_parser.get_format_instructions()}
)

# Schema summary prompt (for text summarization without structured output)
SCHEMA_SUMMARY_PROMPT = PromptTemplate(
template="""You are a Schema Information Summarization Expert. Please organize the following graph database query results into a high-information-entropy text description.

Raw Data:
{raw_data}

Summarization Requirements:
1. Extract key table structure information
2. Identify relationships between tables
3. Highlight important fields and constraints
4. Generate SQL-friendly descriptions

Output Format:
Please return a structured text description including:
- Table structure overview
- Key field descriptions
- Table relationships
- Usage recommendations

Now please summarize the Schema information.""",
    input_variables=["raw_data"]
)