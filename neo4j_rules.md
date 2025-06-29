# Neo4j 数据库建模规则

## 概述

本文档定义了使用Neo4j图数据库对关系型数据库表结构进行建模的规则和标准。

## 节点类型

### 1. Table 节点
- **用途**: 表示数据库中的表
- **属性**:
  - `name`: 表名
  - `dscp`: 表描述

### 2. Column 节点
- **用途**: 表示表中的列
- **属性**:
  - `name`: 列名
  - `dscp`: 列描述

## 关系类型

### 1. Contains 关系
- **方向**: Table → Column
- **含义**: 表示表包含列
- **示例**: `(table:Table)-[:Contains]->(column:Column)`

### 2. Primary_Key 关系
- **方向**: Column → Table
- **含义**: 表示列是表的主键
- **示例**: `(column:Column)-[:Primary_Key]->(table:Table)`

### 3. Foreign_Key 关系
- **方向**: Column → Table
- **含义**: 表示列是外键，引用目标表
- **示例**: `(column:Column)-[:Foreign_Key]->(referenced_table:Table)`

## 建模原则

1. **唯一性**: 每个列只需创建一个Column节点
2. **外键处理**: 作为外键的列节点在原节点基础上额外增加一个Foreign_Key关系指向被引用的表
3. **关系方向**: 外键关系由列节点指向被引用的表节点
4. **关联建立**: 通过外键关系有效建立两个表之间的关联链接

## 示例：电商数据库Schema

### 1. customers 表

```cypher
// 创建customers表节点
CREATE(customers:Table {name:'customers', dscp:'Table of customer information'})

// 创建customers表的列节点
CREATE(customer_id:Column {name:'customer_id', dscp:'客户唯一ID'})
CREATE(name:Column {name:'name', dscp:'姓名'})
CREATE(email:Column {name:'email', dscp:'邮箱'})
CREATE(phone:Column {name:'phone', dscp:'电话'})
CREATE(created_at:Column {name:'created_at', dscp:'注册时间'})
CREATE(vip_level:Column {name:'vip_level', dscp:'VIP等级（如gold, silver）'})

// 建立Contains关系
CREATE(customers)-[:Contains]->(customer_id)
CREATE(customers)-[:Contains]->(name)
CREATE(customers)-[:Contains]->(email)
CREATE(customers)-[:Contains]->(phone)
CREATE(customers)-[:Contains]->(created_at)
CREATE(customers)-[:Contains]->(vip_level)

// 建立Primary_Key关系
CREATE(customer_id)-[:Primary_Key]->(customers)
```

### 2. orders 表

```cypher
// 创建orders表节点
CREATE(orders:Table {name:'orders', dscp:'订单主表'})

// 创建orders表的列节点
CREATE(order_id:Column {name:'order_id', dscp:'订单ID'})
CREATE(order_customer_id:Column {name:'customer_id', dscp:'对应客户'})
CREATE(order_date:Column {name:'order_date', dscp:'下单时间'})
CREATE(status:Column {name:'status', dscp:'订单状态（已发货、已取消）'})
CREATE(shipped_date:Column {name:'shipped_date', dscp:'实际发货时间'})

// 建立Contains关系
CREATE(orders)-[:Contains]->(order_id)
CREATE(orders)-[:Contains]->(order_customer_id)
CREATE(orders)-[:Contains]->(order_date)
CREATE(orders)-[:Contains]->(status)
CREATE(orders)-[:Contains]->(shipped_date)

// 建立Primary_Key关系
CREATE(order_id)-[:Primary_Key]->(orders)

// 建立Foreign_Key关系（customer_id引用customers表）
CREATE(order_customer_id)-[:Foreign_Key]->(customers)
```

### 3. order_items 表

```cypher
// 创建order_items表节点
CREATE(order_items:Table {name:'order_items', dscp:'订单明细表（1个订单多条商品）'})

// 创建order_items表的列节点
CREATE(order_item_id:Column {name:'order_item_id', dscp:'明细记录ID'})
CREATE(item_order_id:Column {name:'order_id', dscp:'所属订单'})
CREATE(item_product_id:Column {name:'product_id', dscp:'商品ID'})
CREATE(quantity:Column {name:'quantity', dscp:'购买数量'})
CREATE(price:Column {name:'price', dscp:'成交价格（可与原价不同）'})

// 建立Contains关系
CREATE(order_items)-[:Contains]->(order_item_id)
CREATE(order_items)-[:Contains]->(item_order_id)
CREATE(order_items)-[:Contains]->(item_product_id)
CREATE(order_items)-[:Contains]->(quantity)
CREATE(order_items)-[:Contains]->(price)

// 建立Primary_Key关系
CREATE(order_item_id)-[:Primary_Key]->(order_items)

// 建立Foreign_Key关系
CREATE(item_order_id)-[:Foreign_Key]->(orders)
CREATE(item_product_id)-[:Foreign_Key]->(products)
```

### 4. products 表

```cypher
// 创建products表节点
CREATE(products:Table {name:'products', dscp:'商品信息表'})

// 创建products表的列节点
CREATE(product_id:Column {name:'product_id', dscp:'商品ID'})
CREATE(product_name:Column {name:'product_name', dscp:'商品名'})
CREATE(product_category_id:Column {name:'category_id', dscp:'商品分类ID'})
CREATE(product_price:Column {name:'price', dscp:'原始单价'})
CREATE(stock:Column {name:'stock', dscp:'当前库存'})
CREATE(product_created_at:Column {name:'created_at', dscp:'上架时间'})

// 建立Contains关系
CREATE(products)-[:Contains]->(product_id)
CREATE(products)-[:Contains]->(product_name)
CREATE(products)-[:Contains]->(product_category_id)
CREATE(products)-[:Contains]->(product_price)
CREATE(products)-[:Contains]->(stock)
CREATE(products)-[:Contains]->(product_created_at)

// 建立Primary_Key关系
CREATE(product_id)-[:Primary_Key]->(products)

// 建立Foreign_Key关系
CREATE(product_category_id)-[:Foreign_Key]->(categories)
```

### 5. categories 表

```cypher
// 创建categories表节点
CREATE(categories:Table {name:'categories', dscp:'商品分类表'})

// 创建categories表的列节点
CREATE(category_id:Column {name:'category_id', dscp:'分类ID'})
CREATE(category:Column {name:'category', dscp:'分类名（如电子、服装）'})

// 建立Contains关系
CREATE(categories)-[:Contains]->(category_id)
CREATE(categories)-[:Contains]->(category)

// 建立Primary_Key关系
CREATE(category_id)-[:Primary_Key]->(categories)
```

### 6. payment 表

```cypher
// 创建payment表节点
CREATE(payment:Table {name:'payment', dscp:'支付信息表'})

// 创建payment表的列节点
CREATE(payment_id:Column {name:'payment_id', dscp:'支付记录ID'})
CREATE(payment_order_id:Column {name:'order_id', dscp:'所属订单'})
CREATE(payment_method:Column {name:'payment_method', dscp:'支付方式（微信、支付宝、信用卡）'})
CREATE(payment_status:Column {name:'payment_status', dscp:'支付状态（已支付、失败）'})
CREATE(payment_date:Column {name:'payment_date', dscp:'支付时间'})

// 建立Contains关系
CREATE(payment)-[:Contains]->(payment_id)
CREATE(payment)-[:Contains]->(payment_order_id)
CREATE(payment)-[:Contains]->(payment_method)
CREATE(payment)-[:Contains]->(payment_status)
CREATE(payment)-[:Contains]->(payment_date)

// 建立Primary_Key关系
CREATE(payment_id)-[:Primary_Key]->(payment)

// 建立Foreign_Key关系
CREATE(payment_order_id)-[:Foreign_Key]->(orders)
```

### 7. shipping 表

```cypher
// 创建shipping表节点
CREATE(shipping:Table {name:'shipping', dscp:'物流信息表'})

// 创建shipping表的列节点
CREATE(shipping_id:Column {name:'shipping_id', dscp:'物流单ID'})
CREATE(shipping_order_id:Column {name:'order_id', dscp:'对应订单'})
CREATE(carrier:Column {name:'carrier', dscp:'物流公司'})
CREATE(tracking_no:Column {name:'tracking_no', dscp:'运单号'})
CREATE(shipped_date:Column {name:'shipped_date', dscp:'发货时间'})
CREATE(delivered_date:Column {name:'delivered_date', dscp:'到货时间'})

// 建立Contains关系
CREATE(shipping)-[:Contains]->(shipping_id)
CREATE(shipping)-[:Contains]->(shipping_order_id)
CREATE(shipping)-[:Contains]->(carrier)
CREATE(shipping)-[:Contains]->(tracking_no)
CREATE(shipping)-[:Contains]->(shipped_date)
CREATE(shipping)-[:Contains]->(delivered_date)

// 建立Primary_Key关系
CREATE(shipping_id)-[:Primary_Key]->(shipping)

// 建立Foreign_Key关系
CREATE(shipping_order_id)-[:Foreign_Key]->(orders)
```

### 8. reviews 表

```cypher
// 创建reviews表节点
CREATE(reviews:Table {name:'reviews', dscp:'用户评价表'})

// 创建reviews表的列节点
CREATE(review_id:Column {name:'review_id', dscp:'评价ID'})
CREATE(review_customer_id:Column {name:'customer_id', dscp:'用户ID'})
CREATE(review_product_id:Column {name:'product_id', dscp:'被评价商品'})
CREATE(rating:Column {name:'rating', dscp:'星级评分（1–5）'})
CREATE(review_text:Column {name:'review_text', dscp:'评论内容'})
CREATE(review_created_at:Column {name:'created_at', dscp:'评论时间'})

// 建立Contains关系
CREATE(reviews)-[:Contains]->(review_id)
CREATE(reviews)-[:Contains]->(review_customer_id)
CREATE(reviews)-[:Contains]->(review_product_id)
CREATE(reviews)-[:Contains]->(rating)
CREATE(reviews)-[:Contains]->(review_text)
CREATE(reviews)-[:Contains]->(review_created_at)

// 建立Primary_Key关系
CREATE(review_id)-[:Primary_Key]->(reviews)

// 建立Foreign_Key关系
CREATE(review_customer_id)-[:Foreign_Key]->(customers)
CREATE(review_product_id)-[:Foreign_Key]->(products)
```

### 9. promotions 表

```cypher
// 创建promotions表节点
CREATE(promotions:Table {name:'promotions', dscp:'促销活动表'})

// 创建promotions表的列节点
CREATE(promotion_id:Column {name:'promotion_id', dscp:'活动ID'})
CREATE(promotion_name:Column {name:'promotion_name', dscp:'活动名'})
CREATE(discount_type:Column {name:'discount_type', dscp:'折扣类型（满减、打折）'})
CREATE(discount_value:Column {name:'discount_value', dscp:'折扣值'})
CREATE(start_date:Column {name:'start_date', dscp:'生效开始'})
CREATE(end_date:Column {name:'end_date', dscp:'生效结束'})

// 建立Contains关系
CREATE(promotions)-[:Contains]->(promotion_id)
CREATE(promotions)-[:Contains]->(promotion_name)
CREATE(promotions)-[:Contains]->(discount_type)
CREATE(promotions)-[:Contains]->(discount_value)
CREATE(promotions)-[:Contains]->(start_date)
CREATE(promotions)-[:Contains]->(end_date)

// 建立Primary_Key关系
CREATE(promotion_id)-[:Primary_Key]->(promotions)
```

### 10. order_promotions 表

```cypher
// 创建order_promotions表节点
CREATE(order_promotions:Table {name:'order_promotions', dscp:'订单与促销的关联（支持多对多）'})

// 创建order_promotions表的列节点
CREATE(op_order_id:Column {name:'order_id', dscp:'订单'})
CREATE(op_promotion_id:Column {name:'promotion_id', dscp:'使用的促销活动'})

// 建立Contains关系
CREATE(order_promotions)-[:Contains]->(op_order_id)
CREATE(order_promotions)-[:Contains]->(op_promotion_id)

// 建立Foreign_Key关系
CREATE(op_order_id)-[:Foreign_Key]->(orders)
CREATE(op_promotion_id)-[:Foreign_Key]->(promotions)
```

### 11. returns 表

```cypher
// 创建returns表节点
CREATE(returns:Table {name:'returns', dscp:'退货记录表'})

// 创建returns表的列节点
CREATE(return_id:Column {name:'return_id', dscp:'退货记录ID'})
CREATE(return_order_item_id:Column {name:'order_item_id', dscp:'退货的订单明细项'})
CREATE(return_date:Column {name:'return_date', dscp:'退货时间'})
CREATE(reason:Column {name:'reason', dscp:'原因描述'})

// 建立Contains关系
CREATE(returns)-[:Contains]->(return_id)
CREATE(returns)-[:Contains]->(return_order_item_id)
CREATE(returns)-[:Contains]->(return_date)
CREATE(returns)-[:Contains]->(reason)

// 建立Primary_Key关系
CREATE(return_id)-[:Primary_Key]->(returns)

// 建立Foreign_Key关系
CREATE(return_order_item_id)-[:Foreign_Key]->(order_items)
```

## 查询示例

### 查询所有表及其列
```cypher
MATCH (t:Table)-[:Contains]->(c:Column)
RETURN t.name as TableName, c.name as ColumnName, c.dscp as Description
ORDER BY t.name, c.name
```

### 查询表之间的外键关系
```cypher
MATCH (c:Column)-[:Foreign_Key]->(t:Table)
RETURN c.name as ForeignKeyColumn, t.name as ReferencedTable
ORDER BY t.name, c.name
```

### 查询特定表的主键
```cypher
MATCH (c:Column)-[:Primary_Key]->(t:Table {name: 'customers'})
RETURN c.name as PrimaryKeyColumn
```

### 查询表之间的关联路径
```cypher
MATCH path = (t1:Table)-[:Contains]->(c:Column)-[:Foreign_Key]->(t2:Table)
WHERE t1.name = 'orders' AND t2.name = 'customers'
RETURN path
```

## 注意事项

1. 每个列只创建一次Column节点，避免重复
2. 外键关系由列节点指向被引用的表节点
3. 主键关系也由列节点指向所属表节点
4. 通过外键关系可以追踪表之间的依赖关系
5. 使用描述性属性便于理解和维护 