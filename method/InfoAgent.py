"""
InfoAgent 的目标可以拆解为：
1. 面向用Query的问题相关性：不是全盘扫描所有结构，而是以Query为导向，收集相关表/字段的信息。
2. 结构摘要压缩（高信息熵）：把图结构压缩成SQLAgent可理解的自然语言描述或结构化摘要。
3. 错误驱动的结构补全：当SQLAgent失败时，自动对图结构进行局部拓展以获取缺失信息。
InfoAgent 可以实现的 API 接口
- get_all_tables()：返回全部表及字段
- get_table_fields(table_name)：返回指定表的字段
- find_tables_by_field(field_name)：字段反向查表
- summarize_related_schema(keywords: List[str])：根据query关键词生成相关schema描述
- suggest_similar_fields(field_name)：根据错误字段提示推荐相似字段及其所在表

其中需要注意的有两点
1. 信息压缩策略（高信息熵文本生成）
2. 错误反馈后的增量探索：SQLAgent在执行出错后，InfoAgent可以根据错误提示反向定位相关字段或表，并：动态拓展图结构探索范围（例如只初始探索部分schema，出错后拓展更多）以及补充字段来源：如出错信息是“column X not found”，则 InfoAgent 应查询图中是否有字段名相似的Field节点，并返回其所属表。
"""