# Snowflake 连接池和重试机制

## 概述

为了解决并发处理时的 Snowflake 连接超时问题，我们实现了一个完整的连接池管理系统和重试机制。该系统能够：

- **连接池管理**：复用数据库连接，避免频繁创建和销毁连接
- **重试机制**：自动重试失败的查询，支持指数退避和随机抖动
- **健康检查**：定期检查连接状态，自动清理不健康的连接
- **统计监控**：提供详细的连接池使用统计信息
- **向后兼容**：与现有代码完全兼容，无需修改调用方式

## 主要特性

### 1. 连接池管理

- **最大连接数控制**：避免过多连接导致的资源耗尽
- **连接复用**：减少连接建立和销毁的开销
- **连接年龄管理**：自动清理过期连接
- **线程安全**：支持多线程并发访问

### 2. 重试机制

- **指数退避**：重试间隔逐渐增加，避免系统过载
- **随机抖动**：避免多个请求同时重试造成的雪崩效应
- **可配置重试次数**：根据需要调整重试策略
- **智能错误分类**：区分不同类型的错误，决定是否重试

### 3. 健康检查

- **定期检查**：后台线程定期检查连接健康状态
- **自动清理**：移除不健康或过期的连接
- **统计监控**：记录健康检查的执行情况

## 使用方法

### 基本使用

```python
from utils.SnowflakeConnectionPool import snowflake_sql_query_with_pool

# 使用连接池执行查询
result = snowflake_sql_query_with_pool(
    sql_query="SELECT * FROM my_table",
    database_id="MY_DATABASE",
    timeout=60,
    log=True
)
```

### 高级配置

```python
from utils.SnowflakeConnectionPool import SnowflakeConnectionPool

# 创建自定义连接池
pool = SnowflakeConnectionPool(
    max_connections=20,           # 最大连接数
    min_connections=5,            # 最小连接数
    connection_timeout=90,        # 连接超时时间（秒）
    max_connection_age=3600,      # 连接最大存活时间（秒）
    health_check_interval=300,    # 健康检查间隔（秒）
    max_retries=3,               # 最大重试次数
    retry_delay=1.0,             # 重试延迟（秒）
    retry_backoff=2.0            # 重试退避倍数
)

# 使用自定义连接池执行查询
result = pool.execute_query_with_retry(
    sql_query="SELECT COUNT(*) FROM large_table",
    database_id="MY_DATABASE",
    log=True
)

# 获取连接池统计信息
stats = pool.get_stats()
print(f"连接池统计: {stats}")

# 关闭连接池
pool.close()
```

### 与现有代码集成

现有的 `snowflake_sql_query` 函数已经自动支持连接池：

```python
from utils.SnowConnect import snowflake_sql_query

# 自动使用连接池（推荐）
result = snowflake_sql_query(
    sql_query="SELECT * FROM my_table",
    database_id="MY_DATABASE",
    timeout=60,
    log=True,
    use_pool=True  # 默认为 True
)

# 强制使用原始连接方式
result = snowflake_sql_query(
    sql_query="SELECT * FROM my_table",
    database_id="MY_DATABASE",
    timeout=60,
    log=True,
    use_pool=False
)
```

## 配置参数说明

### 连接池参数

| 参数                    | 默认值 | 说明                   |
| ----------------------- | ------ | ---------------------- |
| `max_connections`       | 16     | 最大连接数             |
| `min_connections`       | 2      | 最小连接数             |
| `connection_timeout`    | 60     | 连接超时时间（秒）     |
| `max_connection_age`    | 3600   | 连接最大存活时间（秒） |
| `health_check_interval` | 300    | 健康检查间隔（秒）     |

### 重试参数

| 参数            | 默认值 | 说明               |
| --------------- | ------ | ------------------ |
| `max_retries`   | 3      | 最大重试次数       |
| `retry_delay`   | 1.0    | 初始重试延迟（秒） |
| `retry_backoff` | 2.0    | 重试退避倍数       |

### 重试延迟计算

重试延迟采用指数退避 + 随机抖动的策略：

```
延迟时间 = retry_delay × (retry_backoff ^ 重试次数) + 随机抖动
```

例如，使用默认参数：

- 第 1 次重试：1.0 × 2^0 + 抖动 = 1.0 + 抖动
- 第 2 次重试：1.0 × 2^1 + 抖动 = 2.0 + 抖动
- 第 3 次重试：1.0 × 2^2 + 抖动 = 4.0 + 抖动

## 统计信息

连接池提供详细的统计信息：

```python
from utils.SnowflakeConnectionPool import get_pool_stats

stats = get_pool_stats()
print(f"统计信息: {stats}")
```

统计信息包括：

- `total_created`: 总创建连接数
- `total_destroyed`: 总销毁连接数
- `total_borrowed`: 总借用连接数
- `total_returned`: 总归还连接数
- `total_health_checks`: 总健康检查次数
- `total_retries`: 总重试次数
- `current_active`: 当前活跃连接数
- `pool_size`: 当前连接池大小
- `max_connections`: 最大连接数配置
- `min_connections`: 最小连接数配置

## 错误处理

### 常见错误类型

1. **连接超时**：`ReadTimeout`, `ConnectionTimeout`
2. **网络错误**：`NetworkError`, `ConnectionRefused`
3. **SQL 语法错误**：`SyntaxError`, `InvalidSQL`
4. **权限错误**：`PermissionDenied`, `AccessDenied`

### 重试策略

- **网络相关错误**：自动重试
- **临时性错误**：自动重试
- **语法错误**：不重试
- **权限错误**：不重试

## 性能优化建议

### 1. 连接池大小调优

```python
# 根据并发线程数调整连接池大小
max_workers = 16
pool = SnowflakeConnectionPool(
    max_connections=max_workers,  # 与并发线程数匹配
    min_connections=max_workers // 4  # 保持一定的最小连接数
)
```

### 2. 超时时间调优

```python
# 根据查询复杂度调整超时时间
pool = SnowflakeConnectionPool(
    connection_timeout=120,  # 复杂查询需要更长时间
    max_retries=5,          # 增加重试次数
    retry_delay=2.0         # 增加重试延迟
)
```

### 3. 健康检查调优

```python
# 根据连接稳定性调整健康检查
pool = SnowflakeConnectionPool(
    health_check_interval=180,  # 减少检查频率
    max_connection_age=7200     # 增加连接存活时间
)
```

## 测试

运行测试脚本验证连接池功能：

```bash
cd method
python test_connection_pool.py
```

测试包括：

- 基本功能测试
- 并发功能测试
- 重试机制测试
- 健康检查测试
- 集成测试

## 监控和调试

### 启用详细日志

```python
import logging

# 启用DEBUG级别日志
logging.getLogger('utils.SnowflakeConnectionPool').setLevel(logging.DEBUG)
```

### 监控连接池状态

```python
# 定期检查连接池状态
import time
from utils.SnowflakeConnectionPool import get_pool_stats

while True:
    stats = get_pool_stats()
    print(f"连接池状态: 活跃={stats['current_active']}, 池大小={stats['pool_size']}")
    time.sleep(30)
```

## 故障排除

### 常见问题

1. **连接池满了**

   - 症状：`RuntimeError: 连接池已满，无法创建新连接`
   - 解决：增加 `max_connections` 或减少并发线程数

2. **连接超时**

   - 症状：`ReadTimeout` 错误
   - 解决：增加 `connection_timeout` 或检查网络连接

3. **重试次数过多**
   - 症状：查询执行时间过长
   - 解决：调整 `max_retries` 和 `retry_delay`

### 调试技巧

1. **查看连接池统计**：

   ```python
   from utils.SnowflakeConnectionPool import get_pool_stats
   print(get_pool_stats())
   ```

2. **启用详细日志**：

   ```python
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```

3. **监控重试情况**：
   ```python
   # 查看重试统计
   stats = get_pool_stats()
   print(f"总重试次数: {stats['total_retries']}")
   ```

## 最佳实践

1. **合理设置连接池大小**：通常等于或略大于并发线程数
2. **适当的超时时间**：根据查询复杂度设置，避免过短或过长
3. **监控连接池状态**：定期检查统计信息，及时发现问题
4. **正确关闭连接池**：程序结束时调用 `close_global_pool()`
5. **错误处理**：对不同类型的错误采用不同的处理策略
