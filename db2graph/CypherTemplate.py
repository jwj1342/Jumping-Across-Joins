create_node = """
CREATE (n:{label} {{ {properties} }})
"""

create_named_node = """
CREATE ({var}:{label} {{ {properties} }})
"""

match_node = """
MATCH (n:{label} {{ {match_conditions} }})
"""

match_and_return = """
MATCH (n:{label} {{ {match_conditions} }})
RETURN n
"""

update_node = """
MATCH (n:{label} {{ {match_conditions} }})
SET {set_expressions}
"""

delete_node = """
MATCH (n:{label} {{ {match_conditions} }})
DELETE n
"""

create_relationship = """
MATCH (a:{label1} {{ {match1} }})
MATCH (b:{label2} {{ {match2} }})
CREATE (a)-[:{rel_type} {{ {rel_properties} }}]->(b)
"""

create_relationship_return = """
MATCH (a:{label1} {{ {match1} }})
MATCH (b:{label2} {{ {match2} }})
CREATE (a)-[r:{rel_type} {{ {rel_properties} }}]->(b)
RETURN a, r, b
"""

match_relationship = """
MATCH (a:{label1})-[r:{rel_type}]->(b:{label2})
WHERE {where_conditions}
RETURN a, r, b
"""

match_relationship_simple = """
MATCH (a:{label1})-[:{rel_type}]->(b:{label2})
RETURN a, b
"""

aggregate_query = """
MATCH (n:{label})
RETURN {aggregate_function}(n.{property}) AS result
"""

aggregate_multiple = """
MATCH (n:{label})
RETURN COUNT(n) AS count, 
       AVG(n.{property}) AS avg, 
       MIN(n.{property}) AS min, 
       MAX(n.{property}) AS max
"""

count_with_condition = """
MATCH (n:{label})
WHERE {conditions}
RETURN COUNT(n) AS total_count
"""

delete_relationship = """
MATCH (a:{label1})-[r:{rel_type}]->(b:{label2})
WHERE {where_conditions}
DELETE r
"""

detach_delete_node = """
MATCH (n:{label} {{ {match_conditions} }})
DETACH DELETE n
"""

paged_query = """
MATCH (n:{label})
WHERE {conditions}
RETURN n
ORDER BY {order_property} {order_direction}
SKIP {skip} LIMIT {limit}
"""
