#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基于LLM的SQL生成系统
使用LangChain构建Chain结构，支持并发处理
"""

import os
import json
import time
import logging
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from typing import Dict, List, Tuple

import tempfile
import shutil
from utils.init_llm import initialize_llm
from prompts import baseline_prompt_v2 as baseline_prompt
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from dotenv import load_dotenv
from tqdm import tqdm
import jsonlines


# 加载环境变量
load_dotenv(".env")



# 全局锁用于线程安全
file_lock = threading.Lock()
log_lock = threading.Lock()

class SQLGenerationChain:
    """
    基于LangChain的SQL生成器
    使用新的LangChain语法: prompt | llm
    """
    
    def __init__(self, llm):
        self.llm = llm
        self.prompt_template = PromptTemplate(
            input_variables=["query", "database_info"],
            template=baseline_prompt
        )
        # 使用新的LangChain语法: prompt | llm | parser
        self.chain = self.prompt_template | self.llm | StrOutputParser()
    
    def generate_sql(self, query: str, database_info: str) -> Dict[str, str]:
        """
        生成SQL查询
        
        Args:
            query: 自然语言查询
            database_info: 数据库信息
            
        Returns:
            包含SQL的字典
        """
        try:
            result = self.chain.invoke({
                "query": query,
                "database_info": database_info
            })
            
            # 清理SQL结果
            sql = self._clean_sql_result(result)
            
            return {"sql": sql, "success": True, "error": None}
            
        except Exception as e:
            error_msg = f"SQL生成失败: {str(e)}"
            with log_lock:
                logging.error(error_msg)
            return {"sql": f"-- 生成失败: {str(e)}", "success": False, "error": error_msg}
    
    def _clean_sql_result(self, result: str) -> str:
        """
        清理LLM返回的SQL结果
        """
        if not result or not isinstance(result, str):
            logging.warning(f"LLM返回结果为空或非字符串类型: {type(result)}")
            return "-- 生成结果为空"
        
        # 记录原始结果长度
        logging.debug(f"原始LLM返回结果长度: {len(result)} 字符")
        
        original_result = result
        
        # 尝试不同的提取方式
        if "```sql" in result.lower():  # 使用小写进行匹配
            parts = result.split("```sql")
            if len(parts) > 1:
                result = parts[1].split("```")[0]
        elif "```" in result:
            parts = result.split("```")
            if len(parts) > 1:
                result = parts[1]
        
        # 去除多余的空行和空格，但保留SQL格式
        lines = []
        for line in result.split('\n'):
            stripped = line.rstrip()  # 只去除右侧空格，保留缩进
            if stripped:
                lines.append(stripped)
        
        sql = '\n'.join(lines)
        
        # 如果处理后为空，尝试使用原始结果
        if not sql.strip():
            logging.warning("SQL清理后为空，尝试使用原始结果")
            sql = original_result.strip()
        
        # 最终检查
        if not sql.strip():
            logging.error("无法提取有效的SQL语句")
            return "-- SQL生成结果为空"
        
        logging.debug(f"清理后的SQL结果长度: {len(sql)} 字符")
        return sql

# 全局SQL生成器实例（线程安全的复用）
_global_sql_generator = None
_generator_lock = threading.Lock()

def get_sql_generator(llm) -> SQLGenerationChain:
    """
    获取全局SQL生成器实例（线程安全）
    
    Args:
        llm: LLM实例
        
    Returns:
        SQLGenerationChain实例
    """
    global _global_sql_generator
    
    with _generator_lock:
        if _global_sql_generator is None:
            _global_sql_generator = SQLGenerationChain(llm)
            with log_lock:
                logging.info("全局SQL生成器已初始化")
        return _global_sql_generator

def load_database_info(db_id: str, db_info_dir: Path) -> str:
    """
    加载数据库信息
    
    Args:
        db_id: 数据库ID
        db_info_dir: 数据库信息目录
        
    Returns:
        数据库信息字符串
    """
    db_file = db_info_dir / f"{db_id}.txt"
    
    if not db_file.exists():
        with log_lock:
            logging.warning(f"数据库信息文件不存在: {db_file}")
        return f"-- 数据库 {db_id} 的信息文件不存在"
    
    try:
        with open(db_file, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if not content:
                return f"-- 数据库 {db_id} 的信息文件为空"
            return content
    except Exception as e:
        with log_lock:
            logging.error(f"读取数据库信息文件失败 {db_file}: {e}")
        return f"-- 读取数据库 {db_id} 信息失败: {str(e)}"

def create_thread_workspace(thread_id: str, base_temp_dir: Path) -> Path:
    """
    为线程创建独立的临时工作目录
    
    Args:
        thread_id: 线程ID
        base_temp_dir: 基础临时目录
        
    Returns:
        线程工作目录路径
    """
    thread_dir = base_temp_dir / f"thread_{thread_id}"
    thread_dir.mkdir(parents=True, exist_ok=True)
    return thread_dir

def process_single_query(
    item: Dict, 
    llm, 
    db_info_dir: Path, 
    results_dir: Path,
    base_temp_dir: Path,
    timeout_seconds: int = 300
) -> Tuple[str, bool, str]:
    """
    处理单个查询
    
    Args:
        item: 查询项
        llm: LLM实例
        db_info_dir: 数据库信息目录
        results_dir: 结果输出目录
        base_temp_dir: 基础临时目录
        timeout_seconds: 超时时间(秒)
        
    Returns:
        (instance_id, 是否成功, 错误信息)
    """
    thread_id = threading.current_thread().ident
    instance_id = item.get("instance_id", "unknown")
    
    try:
        # 创建线程独立工作目录
        thread_workspace = create_thread_workspace(str(thread_id), base_temp_dir)
        
        with log_lock:
            logging.info(f"开始处理查询: {instance_id}")
        
        # 提取查询信息
        instruction = item.get("instruction", "")
        db_id = item.get("db_id", "")
        
        if not instruction or not db_id:
            raise ValueError(f"查询信息不完整: instruction={bool(instruction)}, db_id={bool(db_id)}")
        
        # 加载数据库信息
        database_info = load_database_info(db_id, db_info_dir)
        
        # 获取复用的SQL生成器
        sql_generator = get_sql_generator(llm)
        
        # 生成SQL（带超时控制）
        start_time = time.time()
        
        # 使用baseline_prompt模板生成SQL
        result = sql_generator.generate_sql(
            query=instruction,
            database_info=database_info
        )
        
        elapsed_time = time.time() - start_time
        
        if elapsed_time > timeout_seconds:
            raise TimeoutError(f"SQL生成超时: {elapsed_time:.2f}秒")
        
        sql = result["sql"]
        success = result["success"]
        
        if not success:
            # 如果生成失败，记录错误但继续保存结果
            with log_lock:
                logging.warning(f"查询 {instance_id} 生成SQL失败，但保存结果: {result.get('error', 'Unknown error')}")
        
        # 保存结果到文件
        output_file = results_dir / f"{instance_id}.sql"
        
        with file_lock:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(f"-- Query: {instruction}\n")
                f.write(f"-- Database: {db_id}\n")
                f.write(f"-- Generated at: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"-- Thread: {thread_id}\n")
                f.write(f"-- Processing time: {elapsed_time:.2f}s\n")
                f.write(f"-- Success: {success}\n")
                if not success and result.get("error"):
                    f.write(f"-- Error: {result['error']}\n")
                f.write("\n")
                f.write(sql)
        
        with log_lock:
            logging.info(f"完成处理查询: {instance_id}, 耗时: {elapsed_time:.2f}秒, 成功: {success}")
        
        return instance_id, success, result.get("error", "") if not success else ""
        
    except TimeoutError as e:
        error_msg = f"超时错误: {str(e)}"
        with log_lock:
            logging.error(f"查询 {instance_id} 处理超时: {error_msg}")
        return instance_id, False, error_msg
        
    except Exception as e:
        error_msg = f"处理错误: {str(e)}"
        with log_lock:
            logging.error(f"查询 {instance_id} 处理失败: {error_msg}")
        return instance_id, False, error_msg
    
    finally:
        # 清理线程工作目录
        try:
            if 'thread_workspace' in locals() and thread_workspace.exists():
                shutil.rmtree(thread_workspace)
        except Exception as e:
            with log_lock:
                logging.warning(f"清理线程工作目录失败: {e}")



def load_queries(input_file: Path) -> List[Dict]:
    """
    加载查询数据
    
    Args:
        input_file: 输入文件路径
        
    Returns:
        查询列表
    """
    queries = []
    
    try:
        with jsonlines.open(input_file) as reader:
            for item in reader:
                queries.append(item)
        
        logging.info(f"成功加载 {len(queries)} 个查询")
        return queries
        
    except Exception as e:
        logging.error(f"加载查询文件失败 {input_file}: {e}")
        return []

def validate_environment() -> bool:
    """
    验证运行环境
    
    Returns:
        环境是否有效
    """
    try:
        # 检查必要的目录
        project_root = Path(__file__).parent
        db_info_dir = project_root / "db_info"
        input_file = project_root / "spider2-snow-crypto.jsonl"
        
        if not input_file.exists():
            logging.error(f"输入文件不存在: {input_file}")
            return False
            
        if not db_info_dir.exists():
            logging.error(f"数据库信息目录不存在: {db_info_dir}")
            return False
        
        # 检查是否有数据库信息文件
        db_files = list(db_info_dir.glob("*.txt"))
        if not db_files:
            logging.error(f"数据库信息目录为空: {db_info_dir}")
            return False
        
        logging.info(f"环境验证通过: 找到 {len(db_files)} 个数据库信息文件")
        return True
        
    except Exception as e:
        logging.error(f"环境验证失败: {e}")
        return False

def main():
    """
    主函数
    """
    # 获取baseline目录路径
    BASELINE_DIR = Path(__file__).parent

    # # 配置日志
    # logging.basicConfig(
    #     level=logging.INFO,
    #     format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
    #     handlers=[
    #         logging.FileHandler(BASELINE_DIR / 'sql_generation.log', encoding='utf-8'),
    #         logging.StreamHandler()
    #     ]
    # )
    class MainThreadOnlyFilter(logging.Filter):
        def filter(self, record):
            return record.threadName == "MainThread"

    # 创建文件 handler（记录所有线程日志）
    file_handler = logging.FileHandler(BASELINE_DIR / 'sql_generation.log', encoding='utf-8')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'))

    # 创建终端 handler，仅主线程
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'))
    console_handler.addFilter(MainThreadOnlyFilter())  # 添加过滤器

    # 设置基本配置
    logging.basicConfig(
        level=logging.INFO,
        handlers=[file_handler, console_handler]
    )
    # 验证环境
    if not validate_environment():
        logging.error("环境验证失败，程序退出")
        return
    
    # 设置路径
    project_root = Path(__file__).parent
    input_file = project_root / "spider2-snow-crypto.jsonl"
    db_info_dir = project_root / "db_info"
    results_dir = project_root / "single_turn_crypto_results"
    
    # 创建必要目录
    results_dir.mkdir(exist_ok=True)
    
    # 创建基础临时目录
    base_temp_dir = Path(tempfile.mkdtemp(prefix="sql_gen_"))
    
    try:
        logging.info("开始SQL生成系统")
        logging.info(f"使用baseline_prompt模板: {len(baseline_prompt)} 字符")
        
        # 初始化LLM
        llm = initialize_llm()
        if llm is None:
            logging.error("LLM初始化失败，程序退出")
            return
        
        # 加载查询数据
        queries = load_queries(input_file)
        if not queries:
            logging.error("没有找到查询数据，程序退出")
            return
        
        # 配置并发参数
        max_workers = min(32, (os.cpu_count() or 1) + 4)  # 默认并发数
        max_workers = int(os.getenv("MAX_WORKERS", max_workers))
        timeout_seconds = int(os.getenv("TIMEOUT_SECONDS", 300))
        
        logging.info(f"开始并发处理，最大工作线程数: {max_workers}, 超时时间: {timeout_seconds}秒")
        
        # 统计信息
        success_count = 0
        failed_count = 0
        failed_items = []
        
        # 使用ThreadPoolExecutor进行并发处理
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_query = {
                executor.submit(
                    process_single_query,
                    item,
                    llm,
                    db_info_dir,
                    results_dir,
                    base_temp_dir,
                    timeout_seconds
                ): item for item in queries
            }
            
            # 使用as_completed异步收集结果
            with tqdm(total=len(queries), desc="处理进度") as pbar:
                for future in as_completed(future_to_query):
                    item = future_to_query[future]
                    
                    try:
                        instance_id, success, error_msg = future.result()
                        
                        if success:
                            success_count += 1
                        else:
                            failed_count += 1
                            failed_items.append({
                                "instance_id": instance_id,
                                "error": error_msg,
                                "item": item
                            })
                        
                        pbar.update(1)
                        pbar.set_postfix({
                            "成功": success_count,
                            "失败": failed_count
                        })
                        
                    except Exception as e:
                        failed_count += 1
                        instance_id = item.get("instance_id", "unknown")
                        failed_items.append({
                            "instance_id": instance_id,
                            "error": f"Future执行异常: {str(e)}",
                            "item": item
                        })
                        pbar.update(1)
        
        # 生成处理报告
        logging.info(f"处理完成！总数: {len(queries)}, 成功: {success_count}, 失败: {failed_count}")
        
        # 保存失败记录
        if failed_items:
            failed_report_file = results_dir / "failed_queries.json"
            with open(failed_report_file, 'w', encoding='utf-8') as f:
                json.dump(failed_items, f, ensure_ascii=False, indent=2)
            logging.info(f"失败记录已保存到: {failed_report_file}")
        
        # 生成汇总报告
        summary_report = {
            "total_queries": len(queries),
            "successful": success_count,
            "failed": failed_count,
            "success_rate": f"{success_count/len(queries)*100:.2f}%",
            "max_workers": max_workers,
            "timeout_seconds": timeout_seconds,
            "prompt_template": "baseline_prompt",
            "prompt_length": len(baseline_prompt),
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        summary_file = results_dir / "summary_report.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary_report, f, ensure_ascii=False, indent=2)
        
        logging.info(f"汇总报告已保存到: {summary_file}")
        logging.info("SQL生成系统运行完成")
        
    except KeyboardInterrupt:
        logging.info("接收到中断信号，正在退出...")
    except Exception as e:
        logging.error(f"系统运行出错: {e}")
    finally:
        # 清理临时目录
        try:
            if base_temp_dir.exists():
                shutil.rmtree(base_temp_dir)
                logging.info("临时目录已清理")
        except Exception as e:
            logging.warning(f"清理临时目录失败: {e}")

if __name__ == "__main__":
    main()
