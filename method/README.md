基于 LangGraph 构建的智能 SQL 生成系统，采用函数式编程架构，通过函数式 InfoAgent 和 SQLAgent 的协作，实现自然语言到 SQL 的自动转换，并支持错误自我修正。

## 系统架构

### 核心组件

1. **通信模块 (Communicate.py)** - 状态、消息和输出 Schema 定义

   - SimpleState：图系统状态定义
   - SchemaInfo：Schema 信息结构
   - SQLExecutionResult：SQL 执行结果
   - SystemState：基础系统状态
   - OutputSchema：输出结果格式定义
   - AgentOutputParser：Agent 输出解析器

2. **主程序模块 (main.py)** - 核心功能实现

   - 图系统节点函数（InfoAgent、SqlAgent 等）
   - 工作流程编排
   - 状态管理
   - 命令行接口

3. **InfoAgent 模块 (InfoAgent.py)** - 数据库 Schema 信息探索函数集

   - prepare_schema_info：准备数据库 Schema 信息
   - get_all_tables：获取所有表信息
   - get_table_fields：获取表字段信息
   - filter_useful_tables：过滤有用的表
   - filter_useful_fields：过滤有用的字段

4. **SQLAgent 模块 (SqlAgent.py)** - SQL 生成函数集
   - process_query：处理用户查询
   - generate_sql：SQL 生成
   - execute_sql：SQL 执行
   - extract_sql_from_response：从响应中提取 SQL
   - clean_sql：清理 SQL 语句

### 工作流程

```mermaid
graph TD
    A[用户输入Query] --> B[InfoAgent探索Schema]
    B --> C[SQLAgent生成SQL]
    C --> D[执行SQL]
    D --> E{执行成功?}
    E -->|是| F[结果验证]
    E -->|否| G[错误分析]
    G --> H[InfoAgent补充信息]
    H --> C
    F --> I{结果为空?}
    I -->|是| J[语义校验]
    I -->|否| K[输出结果]
    J --> H
    K --> L[完成]
```

## 使用方法

### 基本使用

```python
from method.main import run

# 方式1：直接使用主函数
result = run(
    query="What is the total market value of USDC tokens in 2023?",
    database_id="CRYPTO",
    additional_info="",
    save_to_csv=True
)

# 方式2：使用命令行
# python main.py -q "What is the total market value of USDC tokens in 2023?" -d CRYPTO

# 方式3：分别使用各个函数（最大控制）
from method.InfoAgent import prepare_schema_info
from method.SqlAgent import process_query, generate_sql

schema_info = prepare_schema_info("What is the total market value of USDC tokens in 2023?", "CRYPTO")
result = process_query("What is the total market value of USDC tokens in 2023?", schema_info, "CRYPTO")
```

### 命令行使用

```bash
# 运行预定义测试查询（默认模式）
cd method
python main.py

# 运行自定义查询
python main.py --query "What is the total market value of USDC tokens in 2023?" --database CRYPTO

# 使用简化参数
python main.py -q "SELECT * FROM table" -d CRYPTO

# 添加额外信息
python main.py -q "查询语句" -d CRYPTO --additional-info "额外信息"

# 不保存结果到CSV文件
python main.py -q "查询语句" --no-csv
```

### 命令行参数说明

- `--query, -q`: 用户查询语句（可选，不提供则运行预定义测试）
- `--database, -d`: 数据库 ID（默认：CRYPTO）
- `--additional-info, -a`: 额外信息（可选）
- `--no-csv`: 不保存结果到 CSV 文件

## API 接口

### 状态定义 (Communicate.py)

```python
# 使用 Pydantic 模型定义输出格式
class SQLExecutionResult(BaseModel):
    success: bool
    sql_query: str
    result_data: List[Dict[str, Any]]
    error_message: Optional[str] = None
    execution_time: float

class SchemaInfo(BaseModel):
    useful_tables: Dict[str, Any]
    total_tables_count: int
    filtered_tables_count: int
    database_id: str

class SimpleState(BaseModel):
    user_query: str              # 用户查询
    database_id: str             # 数据库ID
    schema_info: Dict[str, Any]  # Schema信息
    generated_sql: str           # 生成的SQL
    execution_result: Dict[str, Any]  # 执行结果
    step: str                    # 当前步骤
    iteration: int               # 当前迭代次数
    max_iterations: int          # 最大迭代次数
    final_sql: str              # 最终SQL
    final_result: List[Dict[str, Any]]  # 最终结果
    error_message: str          # 错误信息
    is_completed: bool          # 是否完成

# 输出格式定义
class OutputSchema(BaseModel):
    success: bool
    final_sql: str
    final_result: List[Dict[str, Any]]
    iterations: int
    execution_time: float
    csv_file: Optional[str] = None
    error_message: Optional[str] = None
```

### InfoAgent 函数 (InfoAgent.py)

```python
# 核心函数
def prepare_schema_info(user_query: str, database_id: str) -> Dict[str, Any]
def get_all_tables(database_id: str) -> Dict[str, Any]
def get_table_fields(table_name: str, database_id: str) -> Dict[str, Any]
def filter_useful_tables(user_query: str, all_tables: Dict[str, Any]) -> List[str]
def filter_useful_fields(user_query: str, table_name: str, table_info: Dict[str, Any]) -> List[str]

# 便捷接口
def process_info_request_simple(user_query: str, database_id: str) -> Dict[str, Any]
```

### SqlAgent 函数 (SqlAgent.py)

```python
# 核心函数
def process_query(user_query: str, schema_info: Dict[str, Any], database_id: str) -> Dict[str, Any]
def generate_sql(user_query: str, schema_info: Dict[str, Any], database_id: str) -> str
def execute_sql(sql_query: str, database_id: str) -> Dict[str, Any]

# 辅助函数
def extract_sql_from_response(response_text: str) -> str
def clean_sql(sql: str) -> str
def validate_sql_basic(sql: str) -> bool

# 便捷接口
def generate_and_execute_sql(user_query: str, schema_info: Dict[str, Any], database_id: str) -> Dict[str, Any]
def quick_sql_test(sql: str, database_id: str) -> Dict[str, Any]
```

### 返回结果格式

```python
# 系统最终输出格式 (OutputSchema)
{
    "success": true,
    "final_sql": "SELECT ...",
    "final_result": [...],
    "iterations": 3,
    "execution_time": 2.5,
    "csv_file": "sql_result_CRYPTO_20231201_143022.csv",
    "error_message": null
}

# SQL执行结果格式 (SQLExecutionResult)
{
    "success": true,
    "sql_query": "SELECT ...",
    "result_data": [...],
    "error_message": null,
    "execution_time": 1.2
}

# Schema信息格式 (SchemaInfo)
{
    "useful_tables": {
        "SCHEMA.TABLE_NAME": {
            "schema": "SCHEMA",
            "table": "TABLE_NAME",
            "useful_fields": ["field1", "field2"],
            "total_fields_count": 10,
            "filtered_fields_count": 2
        }
    },
    "total_tables_count": 50,
    "filtered_tables_count": 3,
    "database_id": "CRYPTO"
}
```

## 配置选项

### 系统参数

```python
# 在main.py中的配置
MAX_ITERATIONS = 3  # 最大迭代次数

# 运行时配置
config = {
    "configurable": {
        "thread_id": "sql_session"
    }
}
```

### 函数配置

```python
# InfoAgent函数配置
info_config = {
    "similarity_threshold": 0.6,  # 相似度阈值
    "max_suggestions": 10,        # 最大建议数量
    "cache_timeout": 300         # 缓存超时（秒）
}

# SQLAgent函数配置
sql_config = {
    "quality_threshold": 0.7,     # 质量分数阈值
    "max_retries": 3,            # 最大重试次数
    "batch_size": 1000           # 批处理大小
}
```

## 错误处理

系统包含完善的错误处理机制：

### 错误类型

- `NETWORK_ERROR`: 网络连接错误
- `DATABASE_ERROR`: 数据库错误
- `SQL_SYNTAX_ERROR`: SQL 语法错误
- `SCHEMA_ERROR`: Schema 相关错误
- `LLM_ERROR`: LLM 调用错误
- `VALIDATION_ERROR`: 验证错误
- `TIMEOUT_ERROR`: 超时错误

### 重试策略

- 网络错误：指数退避重试
- 数据库错误：短延迟重试
- Schema 错误：立即重试
- 语法错误：不重试（需要重新生成）

### 错误恢复

1. **表不存在** → InfoAgent 查找相似表名
2. **字段不存在** → InfoAgent 查找相似字段
3. **语法错误** → SQLAgent 重新生成
4. **结果为空** → 分析查询条件并调整

## 性能监控

系统内置性能监控功能：

```python
from method.error_handler import global_performance_monitor

# 获取性能报告
report = global_performance_monitor.get_performance_report()
print(report)
```

## 日志配置

系统支持详细的日志记录：

```python
import logging

# 配置日志级别
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
```
