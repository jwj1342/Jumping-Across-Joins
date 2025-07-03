# Jumping Across Joins - 智能数据库查询系统

一个基于大语言模型(LLM)的多模态数据库查询生成和优化系统，支持 SQL 生成、图数据库转换和智能查询优化。

## 🎯 项目概述

本项目实现了一个完整的 AI 驱动数据库查询解决方案，主要功能包括：

- **智能 SQL 生成**: 基于自然语言查询生成优化的 Snowflake SQL 语句
- **多轮对话优化**: 支持错误检测和自动修正的多轮交互
- **图数据库转换**: 将关系型数据库 Schema 转换为 Neo4j 图结构
- **高并发处理**: 企业级的并发查询处理能力
- **多数据库支持**: 覆盖 100+个数据库的 Schema 信息

## 🏗️ 系统架构

### 核心模块

```
├── baseline/                    # 核心SQL生成系统
│   ├── single_round.py         # 单轮SQL生成引擎
│   ├── muti_round.py          # 多轮交互SQL生成引擎
│   ├── info_pre.py            # 数据库Schema信息提取
│   ├── prompts.py             # SQL生成提示模板
│   ├── db_info/               # 数据库Schema缓存
│   └── results/               # SQL生成结果
├── utils/                      # 工具模块
│   ├── SnowConnect.py         # Snowflake数据库连接器
│   ├── ExtraDbInfo.py         # 数据库信息提取工具
│   ├── Schema2Cypher.py       # Schema到图数据库转换
│   ├── CypherExecutor.py      # Neo4j图数据库执行器
│   └── prompts.py             # 图数据库相关提示模板
└── resource/                   # 资源文件
    ├── documents/             # 数据库文档和说明
    └── databases/             # 支持的数据库Schema
```
