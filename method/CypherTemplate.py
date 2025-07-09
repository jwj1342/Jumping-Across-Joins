INIT_DB_STRUCTURE_TREE_QUERY = """
MATCH (db:Database {{name: "{database_id}"}})-[:HAS_SCHEMA]->(schema:Schema)-[:HAS_TABLE]->(table:Table)
    WITH db, schema, table
    ORDER BY table.name
    WITH db, schema, COLLECT(table)[..5] AS tables
    UNWIND tables AS table
    OPTIONAL MATCH (table)-[:HAS_UNIQUE_FIELD]->(uf:Field)
    OPTIONAL MATCH (table)-[:USES_FIELD_GROUP]->(group:SharedFieldGroup)-[:HAS_FIELD]->(gf:Field)
    WITH db.name AS dbName, 
         schema.name AS schemaName, 
         table.name AS tableName, 
         COLLECT(DISTINCT uf.name) + COLLECT(DISTINCT gf.name) AS allFields
    WITH dbName, schemaName, tableName, [x IN allFields WHERE x IS NOT NULL] AS fields
    WITH dbName, schemaName, COLLECT({{
      table: tableName,
      fields: fields
    }}) AS tableSummaries
    WITH dbName, COLLECT({{
      schema: schemaName,
      tables: tableSummaries
    }}) AS schemaSummaries
    RETURN {{
      database: dbName,
      schemas: schemaSummaries
    }} AS dbSummary
"""