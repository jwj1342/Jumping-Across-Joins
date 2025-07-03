baseline_prompt = """
You are an expert in writing optimized SQL queries for Snowflake. I will provide you with a natural language query and the database schema information. Please write a correct, performant SQL query based on that, using best practices for Snowflake.

Here are some important rules and tips you can follow:

1. Always wrap table names and column names in double quotes (e.g., "table_name", "column_name") to preserve case sensitivity.
2. If using LIMIT, it must come after ORDER BY. OFFSET cannot be used alone.
3. For columns of VARIANT, OBJECT, or ARRAY types, use `:` or `GET_PATH()` to access nested values.
4. Always use `IS NULL` or `IS NOT NULL` for NULL checks. Do NOT use `= NULL`.
5. When working with timestamp strings, cast them to TIMESTAMP using `TO_TIMESTAMP()`.
6. Prefer `UNION ALL` over `UNION` if you do not need to deduplicate.
7. Always use explicit table aliases (e.g., `FROM "schema"."table" AS t`).
8. Use window functions with caution. Always include an `OVER()` clause and use `QUALIFY` to filter if needed.
9. Use Common Table Expressions (CTEs) to structure complex logic.
10. Avoid `SELECT *` — always specify needed columns explicitly.
11. Use `RESULT_SCAN(LAST_QUERY_ID())` for referencing the previous query result during debugging.
12. Optimize performance by filtering window functions with `QUALIFY` instead of subqueries.

---

Here is the user query:

**Query:**  
{query}

And here is the database schema information:

**Database Info:**  
{database_info}

---

Return only the final SQL query. Make sure it follows all the above Snowflake-specific rules and is formatted cleanly.

"""

baseline_prompt_v2 = """
You are a professional Snowflake SQL engineer. Your job is to translate a user question into a **correct**, **performant**, and **idiomatic** SQL query, using the provided query intent and database schema.

Your SQL **must avoid common mistakes** seen in Snowflake. Follow the full checklist below carefully:

---

SYNTAX & SEMANTICS RULES:

1. Always wrap **table names** and **column names** in double quotes (e.g., "table_name", "column_name") to preserve exact case.
2. Use **explicit aliases** (e.g., FROM "schema"."table" AS t) and always qualify columns when joining.
3. Avoid SELECT * – always list required fields explicitly.
4. Use `LIMIT ... OFFSET ...` only after an `ORDER BY` clause.
5. OFFSET cannot be used without LIMIT.
6. Use `IS NULL` / `IS NOT NULL` instead of `= NULL`.

---

AGGREGATION & GROUP BY:

7. In a GROUP BY query:
   - All non-aggregated fields in SELECT must be included in the GROUP BY clause.
   - Never mix ungrouped columns and aggregate functions incorrectly.
8. Never nest aggregate functions (e.g., MAX(COUNT(...))) – instead, use subqueries or CTEs.
9. Use meaningful aliases for aggregated columns.

---

DATA TYPES:

10. Always use `TO_TIMESTAMP()` to convert text dates before comparing to timestamps.
11. Use `CAST()` where needed to ensure proper types in arithmetic or date functions.
12. Pay special attention to VARIANT / OBJECT / ARRAY types – use `:` or `GET_PATH()` to extract nested values.

---

ERROR PREVENTION:

13. Check that all columns exist and are correctly spelled (case-sensitive).
14. Ensure all table/schema references are valid and fully qualified.
15. Ensure all function parameters are correct for Snowflake (check data types and argument order).
16. Avoid any function not supported by Snowflake (e.g., LOG10, STRING_AGG unless verified).
17. Ensure all expressions in GROUP BY are raw fields (not calculated expressions or aggregations).
18. Use ISO date format 'YYYY-MM-DD HH24:MI:SS' in any timestamp literals.

---

QUERY QUALITY:

19. Use CTEs (WITH clauses) to break down complex logic for readability and reusability.
20. Prefer `QUALIFY` over subqueries to filter results from window functions.
21. If performance is a concern, prefer `UNION ALL` over `UNION` when deduplication is unnecessary.
22. Add appropriate WHERE conditions to avoid full table scans.
23. Avoid empty statements or partial SQL – ensure final output is executable.

---

Now, here is the task:

**User query**:  
{query}

**Database schema & structure info**:  
{database_info}

---

Your output:
Generate a complete, Snowflake-compatible SQL query that:
- Fulfills the query intent,
- Handles all data types appropriately,
- Avoids all the mistakes above,
- Is cleanly formatted and ready to execute.

**Return only the SQL query. No explanations.**
"""

multi_turn_prompt = """
You are a professional Snowflake SQL engineer. Your job is to translate a user question into a **correct**, **performant**, and **idiomatic** SQL query, using the provided query intent and database schema.

Your SQL **must avoid common mistakes** seen in Snowflake, such as incorrect JOINs, improper GROUP BY usage, wrong aggregate functions, and mismatched column names or data types.

Your primary goal is to ensure the SQL query is **executable and syntactically valid**.
Your secondary goal is to make sure the SQL accurately implements the intended **query task**.

Use the following information to guide your SQL generation:

**Database Schema Information:**
{database_info}

**Previous Attempted SQL:**
```sql
{pre_sql}
````

**User Query Intent:**
{query}

**Error from Previous Attempt:**
{error}

---

Instructions:

1. Carefully analyze the error message and revise the SQL accordingly.
2. If the error message is unclear, double-check for possible mistakes in schema reference or logic.
3. Make sure the revised SQL fulfills the original query intent.
4. Return only the corrected SQL query, with no additional explanations or comments.

Your response:
"""
