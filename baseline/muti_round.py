#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基于单轮系统的多轮交互SQL生成系统
在单轮生成的基础上增加SQL检查和修正功能，最多进行3次修正循环
"""

import os
import json
import time
import logging
import threading
from pathlib import Path
from typing import Dict, Tuple
from dotenv import load_dotenv
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser

# 加载环境变量
load_dotenv(".env")

# 导入单轮系统的功能
from single_round import (
    SQLGenerationChain,
    load_database_info,
    load_queries,
    validate_environment,
    create_thread_workspace,
    file_lock,
    log_lock
)
from utils.init_llm import initialize_llm
# 导入Snowflake连接模块
import sys
# 将项目根目录添加到Python路径中
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
from utils.SnowConnect import snowflake_sql_query


# 导入prompt模板
from prompts import multi_turn_prompt

# 获取baseline目录路径
BASELINE_DIR = Path(__file__).parent

class MultiTurnSQLGenerationChain:
    """
    多轮交互SQL生成器
    基于单轮生成器，增加错误检查和修正功能
    """
    
    def __init__(self, llm):
        self.llm = llm
        # 复用单轮生成器进行初始SQL生成
        self.single_turn_generator = SQLGenerationChain(llm)
        
        # 多轮修正的prompt模板
        self.multi_turn_template = PromptTemplate(
            input_variables=["query", "database_info", "pre_sql", "error"],
            template=multi_turn_prompt
        )
        # 创建多轮修正链
        self.correction_chain = self.multi_turn_template | self.llm | StrOutputParser()
    
    def generate_sql_with_correction(
        self, 
        query: str, 
        database_info: str, 
        database_id: str,
        max_rounds: int = 3
    ) -> Dict[str, any]:
        """
        生成SQL并进行多轮修正
        
        Args:
            query: 自然语言查询
            database_info: 数据库信息
            database_id: 数据库ID
            max_rounds: 最大修正轮数
            
        Returns:
            包含SQL、成功状态、错误信息、轮数等的字典
        """
        # 第一轮：使用单轮生成器生成初始SQL
        result = self.single_turn_generator.generate_sql(query, database_info)
        
        if not result["success"]:
            return {
                "sql": result["sql"],
                "success": False,
                "error": result["error"],
                "rounds": 0,
                "correction_history": []
            }
        
        current_sql = result["sql"]
        correction_history = []
        
        # 多轮修正循环
        for round_num in range(1, max_rounds + 1):
            # 检查当前SQL
            check_result = check_sql(current_sql, database_id)
            
            if check_result == "success":
                # SQL检查通过，返回成功结果
                return {
                    "sql": current_sql,
                    "success": True,
                    "error": None,
                    "rounds": round_num,
                    "correction_history": correction_history
                }
            
            # SQL检查失败，记录历史并尝试修正
            correction_history.append({
                "round": round_num,
                "sql": current_sql,
                "error": check_result
            })
            
            with log_lock:
                logging.info(f"第{round_num}轮SQL检查失败，开始修正: {check_result}")
            
            # 如果已达到最大轮数，不再修正
            if round_num >= max_rounds:
                break
            
            try:
                # 使用多轮prompt进行修正
                corrected_result = self.correction_chain.invoke({
                    "query": query,
                    "database_info": database_info,
                    "pre_sql": current_sql,
                    "error": check_result
                })
                
                # 清理修正后的SQL
                current_sql = self.single_turn_generator._clean_sql_result(corrected_result)
                
            except Exception as e:
                error_msg = f"第{round_num + 1}轮SQL修正失败: {str(e)}"
                with log_lock:
                    logging.error(error_msg)
                
                return {
                    "sql": current_sql,
                    "success": False,
                    "error": error_msg,
                    "rounds": round_num,
                    "correction_history": correction_history
                }
        
        # 达到最大轮数仍未通过检查
        final_check = check_sql(current_sql, database_id)
        return {
            "sql": current_sql,
            "success": final_check == "success",
            "error": final_check if final_check != "success" else None,
            "rounds": max_rounds,
            "correction_history": correction_history
        }

def check_sql(sql: str, database_id: str) -> str:
    """
    检查sql语句的正确性
    
    Args:
        sql: 待检查的sql语句
        database_id: 数据库ID
        
    Returns:
        str: 检查成功返回'success',失败返回具体错误原因
    """
    if not sql or not sql.strip():
        return "SQL不能为空"
    
    if not database_id or not database_id.strip():
        return "数据库ID不能为空"
    
    try:
        # 使用EXPLAIN语句验证SQL语法正确性，避免实际执行可能耗时的查询
        # EXPLAIN不会执行查询，只会验证语法和生成执行计划
        explain_sql = f"EXPLAIN {sql.strip()}"
        
        # 设置较短的超时时间，因为这只是语法检查
        with log_lock:
            logging.debug(f"检查SQL语法: {database_id}")
            
        result = snowflake_sql_query(
            sql_query=explain_sql,
            database_id=database_id,
            timeout=15  # 15秒超时，足够进行语法检查
        )
        
        # 如果EXPLAIN成功执行，说明SQL语法正确
        with log_lock:
            logging.debug(f"SQL语法检查通过: {database_id}")
        
        return "success"
        
    except Exception as e:
        error_msg = str(e)
        
        with log_lock:
            logging.debug(f"SQL语法检查失败: {database_id}, 错误: {error_msg}")
        
        # 提取详细的SQL错误信息
        if "SQL compilation error" in error_msg:
            # 尝试提取具体的SQL编译错误信息
            import re
            
            # 查找具体的错误描述
            # 匹配模式如: "error line X at position Y" 和后续的错误描述
            line_pos_pattern = r"error line (\d+) at position (\d+)"
            line_pos_match = re.search(line_pos_pattern, error_msg)
            
            # 提取错误描述，通常在最后一行或者在特定关键词后
            error_lines = error_msg.split('\n')
            detailed_error = ""
            
            for line in error_lines:
                line = line.strip()
                # 跳过一些通用信息行
                if (line and 
                    "执行Snowflake查询时发生错误" not in line and
                    "SQL compilation error:" not in line and
                    not line.startswith("000904") and
                    not line.startswith("Snowflake连接或查询错误")):
                    detailed_error = line
                    break
            
            # 构建详细错误信息
            if line_pos_match and detailed_error:
                line_num = line_pos_match.group(1)
                pos_num = line_pos_match.group(2)
                return f"SQL编译错误 (第{line_num}行第{pos_num}位置): {detailed_error}"
            elif detailed_error:
                return f"SQL编译错误: {detailed_error}"
            else:
                return "SQL编译错误: 语法无效"
                
        elif "timeout" in error_msg.lower():
            return "SQL检查超时"
        elif "connection" in error_msg.lower():
            return "数据库连接失败"
        elif "authentication" in error_msg.lower():
            return "数据库认证失败" 
        elif "does not exist" in error_msg.lower():
            return "表或列不存在"
        elif "permission" in error_msg.lower() or "privilege" in error_msg.lower():
            return "权限不足"
        else:
            # 对于其他错误，也尝试提取有用信息
            error_lines = error_msg.split('\n')
            for line in error_lines:
                line = line.strip()
                if (line and 
                    "执行Snowflake查询时发生错误" not in line and
                    not line.startswith("Snowflake连接或查询错误")):
                    return f"SQL检查失败: {line[:150]}..."
            
            return f"SQL检查失败: {error_msg[:100]}..."

def process_single_query_multi_turn(
    item: Dict, 
    llm, 
    db_info_dir: Path, 
    results_dir: Path,
    base_temp_dir: Path,
    timeout_seconds: int = 300,
    max_rounds: int = 3
) -> Tuple[str, bool, str, int]:
    """
    处理单个查询（多轮版本）
    
    Args:
        item: 查询项
        llm: LLM实例
        db_info_dir: 数据库信息目录
        results_dir: 结果输出目录
        base_temp_dir: 基础临时目录
        timeout_seconds: 超时时间(秒)
        max_rounds: 最大修正轮数
        
    Returns:
        (instance_id, 是否成功, 错误信息, 修正轮数)
    """
    thread_id = threading.current_thread().ident
    instance_id = item.get("instance_id", "unknown")
    
    try:
        # 创建线程独立工作目录
        thread_workspace = create_thread_workspace(str(thread_id), base_temp_dir)
        
        with log_lock:
            logging.info(f"开始多轮处理查询: {instance_id}")
        
        # 提取查询信息
        instruction = item.get("instruction", "")
        db_id = item.get("db_id", "")
        
        if not instruction or not db_id:
            raise ValueError(f"查询信息不完整: instruction={bool(instruction)}, db_id={bool(db_id)}")
        
        # 加载数据库信息
        database_info = load_database_info(db_id, db_info_dir)
        
        # 创建多轮SQL生成器
        multi_turn_generator = MultiTurnSQLGenerationChain(llm)
        
        # 生成SQL（带超时控制）
        start_time = time.time()
        
        result = multi_turn_generator.generate_sql_with_correction(
            query=instruction,
            database_info=database_info,
            database_id=db_id,
            max_rounds=max_rounds
        )
        
        elapsed_time = time.time() - start_time
        
        if elapsed_time > timeout_seconds:
            raise TimeoutError(f"SQL生成超时: {elapsed_time:.2f}秒")
        
        sql = result["sql"]
        success = result["success"]
        rounds = result["rounds"]
        correction_history = result.get("correction_history", [])
        
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
                f.write(f"-- Correction rounds: {rounds}\n")
                
                if not success and result.get("error"):
                    f.write(f"-- Final error: {result['error']}\n")
                
                # 记录修正历史
                if correction_history:
                    f.write(f"-- Correction history:\n")
                    for i, hist in enumerate(correction_history):
                        f.write(f"--   Round {hist['round']}: {hist['error']}\n")
                
                f.write("\n")
                f.write(sql)
        
        with log_lock:
            logging.info(f"完成多轮处理查询: {instance_id}, 耗时: {elapsed_time:.2f}秒, 成功: {success}, 轮数: {rounds}")
        
        return instance_id, success, result.get("error", "") if not success else "", rounds
        
    except TimeoutError as e:
        error_msg = f"超时错误: {str(e)}"
        with log_lock:
            logging.error(f"查询 {instance_id} 处理超时: {error_msg}")
        return instance_id, False, error_msg, 0
        
    except Exception as e:
        error_msg = f"处理错误: {str(e)}"
        with log_lock:
            logging.error(f"查询 {instance_id} 处理失败: {error_msg}")
        return instance_id, False, error_msg, 0
    
    finally:
        # 清理线程工作目录
        try:
            if 'thread_workspace' in locals() and thread_workspace.exists():
                import shutil
                shutil.rmtree(thread_workspace)
        except Exception as e:
            with log_lock:
                logging.warning(f"清理线程工作目录失败: {e}")

def main():
    """
    主函数（多轮版本）
    """
    # 配置日志
    # logging.basicConfig(
    #     level=logging.INFO,
    #     format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
    #     handlers=[
    #         logging.FileHandler(BASELINE_DIR / 'multi_turn_sql_generation.log', encoding='utf-8'),
    #         logging.StreamHandler()
    #     ]
    # )
    # 自定义过滤器：只允许主线程日志输出
    class MainThreadOnlyFilter(logging.Filter):
        def filter(self, record):
            return record.threadName == "MainThread"

    # 创建文件 handler（记录所有线程日志）
    file_handler = logging.FileHandler(BASELINE_DIR / 'multi_turn_sql_generation.log', encoding='utf-8')
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
    results_dir = project_root / "multi_turn_crypto_results"
    
    # 创建必要目录
    results_dir.mkdir(exist_ok=True)
    
    # 创建基础临时目录
    import tempfile
    import shutil
    base_temp_dir = Path(tempfile.mkdtemp(prefix="multi_turn_sql_gen_"))

    
    try:
        logging.info("开始多轮SQL生成系统")
        logging.info(f"使用multi_turn_prompt模板: {len(multi_turn_prompt)} 字符")
        

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
        max_workers = min(32, (os.cpu_count() or 1) + 4)
        max_workers = int(os.getenv("MAX_WORKERS", max_workers))
        timeout_seconds = int(os.getenv("TIMEOUT_SECONDS", 300))
        max_rounds = int(os.getenv("MAX_CORRECTION_ROUNDS", 10))
        
        logging.info(f"开始并发处理，最大工作线程数: {max_workers}, 超时时间: {timeout_seconds}秒, 最大修正轮数: {max_rounds}")
        
        # 统计信息
        success_count = 0
        failed_count = 0
        first_round_success_count = 0  # 第一轮就成功的数量
        corrected_success_count = 0    # 修正后成功的数量
        failed_items = []
        total_rounds = 0
        
        # 使用ThreadPoolExecutor进行并发处理
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from tqdm import tqdm
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_query = {
                executor.submit(
                    process_single_query_multi_turn,
                    item,
                    llm,
                    db_info_dir,
                    results_dir,
                    base_temp_dir,
                    timeout_seconds,
                    max_rounds
                ): item for item in queries
            }
            
            # 收集结果
            with tqdm(total=len(queries), desc="多轮处理进度") as pbar:
                for future in as_completed(future_to_query):
                    item = future_to_query[future]
                    
                    try:
                        instance_id, success, error_msg, rounds = future.result()
                        total_rounds += rounds
                        
                        if success:
                            success_count += 1
                            if rounds == 1:
                                first_round_success_count += 1
                            else:
                                corrected_success_count += 1
                        else:
                            failed_count += 1
                            failed_items.append({
                                "instance_id": instance_id,
                                "error": error_msg,
                                "rounds": rounds,
                                "item": item
                            })
                        
                        pbar.update(1)
                        pbar.set_postfix({
                            "一次成功": first_round_success_count,
                            "修正成功": corrected_success_count,
                            "失败": failed_count,
                            "平均轮数": f"{total_rounds/(success_count+failed_count):.1f}" if (success_count+failed_count) > 0 else "0"
                        })
                        
                    except Exception as e:
                        failed_count += 1
                        instance_id = item.get("instance_id", "unknown")
                        failed_items.append({
                            "instance_id": instance_id,
                            "error": f"Future执行异常: {str(e)}",
                            "rounds": 0,
                            "item": item
                        })
                        pbar.update(1)
                        pbar.set_postfix({
                            "一次成功": first_round_success_count,
                            "修正成功": corrected_success_count,
                            "失败": failed_count,
                            "平均轮数": f"{total_rounds/(success_count+failed_count):.1f}" if (success_count+failed_count) > 0 else "0"
                        })
        
        # 生成处理报告
        avg_rounds = total_rounds / len(queries) if len(queries) > 0 else 0
        logging.info(f"多轮处理完成！总数: {len(queries)}, 一次成功: {first_round_success_count}, 修正后成功: {corrected_success_count}, 失败: {failed_count}, 平均修正轮数: {avg_rounds:.2f}")
        
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
            "first_round_success": first_round_success_count,
            "corrected_success": corrected_success_count,
            "failed": failed_count,
            "success_rate": f"{success_count/len(queries)*100:.2f}%",
            "first_round_success_rate": f"{first_round_success_count/len(queries)*100:.2f}%",
            "correction_success_rate": f"{corrected_success_count/len(queries)*100:.2f}%",
            "correction_effectiveness": f"{corrected_success_count/(corrected_success_count+failed_count)*100:.2f}%" if (corrected_success_count+failed_count) > 0 else "N/A",
            "max_workers": max_workers,
            "timeout_seconds": timeout_seconds,
            "max_correction_rounds": max_rounds,
            "average_rounds": f"{avg_rounds:.2f}",
            "total_correction_rounds": total_rounds,
            "prompt_template": "multi_turn_prompt",
            "prompt_length": len(multi_turn_prompt),
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        summary_file = results_dir / "summary_report.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary_report, f, ensure_ascii=False, indent=2)
        
        logging.info(f"汇总报告已保存到: {summary_file}")
        logging.info("多轮SQL生成系统运行完成")
        
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
