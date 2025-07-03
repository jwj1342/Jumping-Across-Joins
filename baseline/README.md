该目录下实现了两个 SQL 生成 baseline 系统，用于比较单轮交互与多轮交互在复杂查询场景下的性能表现。

## 🎯 项目目标

建立两个基准系统来评估 LLM 在 SQL 生成任务中的表现：

- **单轮交互 Baseline**: 基于单次对话生成 SQL 查询
- **多轮交互 Baseline**: 支持多轮对话优化和修正 SQL 查询

## 🏗️ 系统架构

### 核心模块

1. **数据准备模块** (`info_pre.py`)

   - 自动提取数据库 Schema 信息
   - 并发处理多个数据库
   - 生成结构化的数据库描述文件

2. **单轮 SQL 生成引擎** (`baseline/single_round.py`)

3. **多轮交互 SQL 生成引擎** (`baseline/multi_round.py`)

### 关键特性

- ⚡ **高并发处理**: 使用线程池优化处理速度
- 🔒 **线程安全**: 独立工作目录避免冲突
- ⏱️ **超时控制**: 防止长时间阻塞
- 📊 **异步处理**: 实时收集和显示结果

## 🚀 快速开始

### 1. 环境准备

```bash
# 安装依赖
pip install -r requirements.txt

# 配置环境变量
cp .env.example .env
# 编辑 .env 文件
```

### 2. 数据准备

````bash
# 提取数据库Schema信息
python info_pre.py


数据准备完成后，会在 `db_info/` 目录下生成各数据库的 Schema 文件。

### 3. 运行 SQL 生成

```bash
# 运行主系统
python *_round.py
````

## ⚙️ 配置说明

### 环境变量

| 变量名            | 必需 | 默认值                    | 说明            |
| ----------------- | ---- | ------------------------- | --------------- |
| `OPENAI_API_KEY`  | ✅   | -                         | OpenAI API 密钥 |
| `OPENAI_BASE_URL` | ❌   | https://api.openai.com/v1 | API 基础地址    |
| `OPENAI_MODEL`    | ❌   | gpt-3.5-turbo-instruct    | 使用模型        |
| `MAX_WORKERS`     | ❌   | min(32, cpu_count+4)      | 并发线程数      |
| `TIMEOUT_SECONDS` | ❌   | 300                       | 查询超时时间    |

### 性能调优

- **并发线程数**: 根据系统性能调整 `MAX_WORKERS`
- **超时时间**: 复杂查询可适当增加 `TIMEOUT_SECONDS`
- **数据库连接**: 确保 Snowflake 访问权限正常

## 📁 项目结构

```
baseline/
├── main.py                    # SQL生成主程序
├── info_pre.py               # 数据库信息提取（完整版）
├── test_info_pre.py          # 数据库信息提取（测试版）
├── quick_test.py             # 环境测试工具
├── requirements.txt          # 依赖包列表
├── env_example.txt          # 环境变量示例
├── spider2-snow.jsonl       # 输入数据集
├── db_info/                 # 数据库Schema输出
│   ├── GA4.txt
│   ├── GA360.txt
│   └── ...
├── *output*/                  # SQL生成结果
│   ├── *.sql               # 生成的SQL文件
│   ├── failed_queries.json # 失败记录
│   └── summary_report.json # 汇总报告
└── utils/                   # 工具模块
    └── ExtraDbInfo.py      # 数据库信息提取工具
```

## 📊 输入输出

### 输入格式

`spider2-snow.jsonl` 文件，每行包含：

```json
{
  "instance_id": "sf_bq011",
  "instruction": "How many distinct pseudo users had positive engagement time...",
  "db_id": "GA4",
  "external_knowledge": "ga4_obfuscated_sample_ecommerce.events.md"
}
```

### 输出文件

- **SQL 文件**: 每个查询对应一个 `.sql` 文件
- **汇总报告**: 处理统计和成功率
- **失败记录**: 详细的错误信息

## 🔧 使用指南

### 故障排除

**数据库连接问题**

- 检查 Snowflake 访问权限
- 确认网络连接正常

**API 调用失败**

- 验证 `OPENAI_API_KEY` 配置
- 检查 API 余额和限制

**性能问题**

- 适当调整 `MAX_WORKERS` 数量
- 增加 `TIMEOUT_SECONDS` 设置
