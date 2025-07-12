TABLE_BASED_DB_STRUCTURE_TREE_QUERY = """
MATCH (db:Database {name: $database_id})
MATCH (db)-[:HAS_SCHEMA]->(schema:Schema)-[:HAS_TABLE]->(table:Table)
WHERE table.name IN $table_names

// 获取表的所有字段 - 两种方式
// 方式1: 直接字段关系 Table -[HAS_UNIQUE_FIELD]-> Field
OPTIONAL MATCH (table)-[:HAS_UNIQUE_FIELD]->(uf:Field)

// 方式2: 通过共享字段组 Table -[USES_FIELD_GROUP]-> SharedFieldGroup -[HAS_FIELD]-> Field
OPTIONAL MATCH (table)-[:USES_FIELD_GROUP]->(sfg:SharedFieldGroup)-[:HAS_FIELD]->(gf:Field)

// 构建字段详情，包含所有字段信息
WITH db.name AS dbName,
     schema.name AS schemaName, 
     table.name AS tableName,
     COLLECT(DISTINCT {
       name: uf.name,
       type: uf.type,
       description: COALESCE(uf.description, ""),
       field_id: uf.field_id
     }) + COLLECT(DISTINCT {
       name: gf.name,
       type: gf.type,
       description: COALESCE(gf.description, ""),
       field_id: gf.field_id
     }) AS allFieldDetails

// 过滤并清理字段数据
WITH dbName, schemaName, tableName,
     [field IN allFieldDetails WHERE field.name IS NOT NULL] AS cleanFieldDetails

// 确保有字段数据才包含在结果中
WHERE size(cleanFieldDetails) > 0

WITH dbName, schemaName, COLLECT({
  table: tableName,
  fields: cleanFieldDetails
}) AS tableSummaries

WITH dbName, COLLECT({
  schema: schemaName,
  tables: tableSummaries
}) AS schemaSummaries

RETURN {
  database: dbName,
  schemas: schemaSummaries
} AS dbSummary
"""