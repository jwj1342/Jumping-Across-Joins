"""
Neo4j数据库建模规则和提示模板
"""

# Neo4j建模规则（简略版）
NEO4J_MODELING_RULES = """
# Neo4j 数据库建模规则 (简略版)

## 建模步骤（严格按顺序执行）

### 阶段一：节点创建
1. **创建所有Table节点**: `CREATE (table:Table {name: 'TABLE_NAME', dscp: 'description'})`
2. **创建所有Column节点**: `CREATE (column:Column {name: 'COLUMN_NAME', dscp: 'description'})`

### 阶段二：关系创建（使用MATCH语句）
3. **创建Contains关系**: `MATCH (t:Table {name: 'TABLE_NAME'}), (c:Column {name: 'COLUMN_NAME'}) CREATE (t)-[:Contains]->(c)`
4. **创建Primary_Key关系（可选）**: `MATCH (c:Column {name: 'PK_COL'}), (t:Table {name: 'TABLE_NAME'}) CREATE (c)-[:Primary_Key]->(t)`

## 节点类型
- **Table节点**: 表示数据库表，属性包含name和dscp
- **Column节点**: 表示表中的列，属性包含name和dscp

## 关系类型
- **Contains**: Table → Column，表示表包含列
- **Primary_Key**: Column → Table，表示列是表的主键
- **Foreign_Key**: Column → Table，表示列是外键，引用目标表
- **HiddenRelation**: Table ↔ Table，表示表之间的隐式关系（双向）

## 核心原则
1. **节点创建顺序**: 先所有Table节点，再所有Column节点
2. **关系创建方式**: 必须使用MATCH语句查找节点，然后创建关系
3. **必要关系**: 每个Column必须通过Contains关系连接到Table
4. **当前阶段重点**: 表-列基本结构，暂不处理外键关系

## 标准模板
```
// 阶段一：创建节点
CREATE (table1:Table {name: 'TABLE1', dscp: 'description'});
CREATE (col1:Column {name: 'COL1', dscp: 'description'});

// 阶段二：创建关系
MATCH (t:Table {name: 'TABLE1'}), (c:Column {name: 'COL1'})
CREATE (t)-[:Contains]->(c);
```
"""

# 生成Cypher查询的主要prompt模板
CYPHER_GENERATION_PROMPT = """
You are a helpful assistant that generates Cypher queries based on the given schema and rules.
The schema describes the structure of the table database, which we need to transform into a graph database.
The rules contain details about the representation of the schema in the graph database.

Here is the schema: {schema_info}
Here are the rules: {rules}

🎯 CURRENT PHASE FOCUS: Table-Column Basic Structure Only

CRITICAL REQUIREMENTS (MUST FOLLOW EXACTLY):

🔹 PHASE 1: Create ALL nodes first
1. Create ALL Table nodes first (for every table in the schema)
2. Create ALL Column nodes second (for every column in the schema)

🔹 PHASE 2: Create relationships using MATCH statements
3. **MUST use MATCH to find nodes before creating relationships**
4. **MUST create Contains relationships**: For EVERY column, create (table)-[:Contains]->(column) relationship
5. **OPTIONAL: Primary_Key relationships**: Only if you can clearly identify primary keys

🚫 WHAT NOT TO DO (CURRENT PHASE):
- Do NOT create Foreign_Key relationships (skip this for now)
- Do NOT try to establish table-to-table relationships
- Do NOT use variables from previous CREATE statements (they are out of scope!)

EXECUTION ORDER TEMPLATE:
```
// =====================================
// PHASE 1: Create ALL nodes first
// =====================================
CREATE (table1:Table {{name: 'TABLE1', dscp: 'description'}});
CREATE (table2:Table {{name: 'TABLE2', dscp: 'description'}});
// ... ALL table nodes

CREATE (col1:Column {{name: 'COL1', dscp: 'description'}});
CREATE (col2:Column {{name: 'COL2', dscp: 'description'}});
// ... ALL column nodes

// =====================================
// PHASE 2: Create relationships using MATCH
// =====================================
MATCH (t:Table {{name: 'TABLE1'}}), (c:Column {{name: 'COL1'}})
CREATE (t)-[:Contains]->(c);

MATCH (t:Table {{name: 'TABLE1'}}), (c:Column {{name: 'COL2'}})
CREATE (t)-[:Contains]->(c);

// ... ALL Contains relationships using MATCH statements
```

🔧 CRITICAL FIX: Variables from CREATE statements are NOT available in subsequent statements.
You MUST use MATCH to find nodes by their properties before creating relationships!

⚠️ CRITICAL: Without MATCH statements, the relationships will fail to create!

IMPORTANT: Return a valid JSON object with a single "cypher" field containing ALL the Cypher statements concatenated with semicolons and newlines.
Do NOT create multiple "cypher" fields. Use a single field with all statements.

Example response format:
{{"cypher": "CREATE (table1:Table {{name: 'table1', dscp: 'Table description'}});\\nCREATE (col1:Column {{name: 'col1', dscp: 'Column description'}});\\nMATCH (t:Table {{name: 'table1'}}), (c:Column {{name: 'col1'}}) CREATE (t)-[:Contains]->(c);"}}
"""


def get_cypher_generation_prompt(schema_info: str, rules: str) -> str:
    """
    获取生成Cypher查询的完整prompt

    Args:
        schema_info: 数据库schema信息
        rules: 建模规则

    Returns:
        格式化后的prompt字符串
    """
    return CYPHER_GENERATION_PROMPT.format(schema_info=schema_info, rules=rules)


def get_neo4j_rules() -> str:
    """
    获取Neo4j建模规则

    Returns:
        建模规则字符串
    """
    return NEO4J_MODELING_RULES
