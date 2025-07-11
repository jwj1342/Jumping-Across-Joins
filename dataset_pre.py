#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据预处理脚本：过滤 spider2-snow.jsonl 文件，只保留指定 db_id 的数据行
"""

import json
import os
import argparse
from pathlib import Path

def filter_database_data(input_file: str, output_file: str, target_db_id: str) -> None:
    """
    过滤 JSONL 文件，只保留指定 db_id 的数据行
    
    Args:
        input_file (str): 输入文件路径
        output_file (str): 输出文件路径
        target_db_id (str): 目标数据库ID
    """
    filtered_lines = []
    total_lines = 0
    
    # 读取原始文件
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            total_lines += 1
            try:
                data = json.loads(line.strip())
                if data.get('db_id') == target_db_id:
                    filtered_lines.append(data)
            except json.JSONDecodeError as e:
                print(f"警告：跳过无效的 JSON 行 (行 {total_lines}): {e}")
                continue
    
    # 写入过滤后的数据
    with open(output_file, 'w', encoding='utf-8') as f:
        for data in filtered_lines:
            json_line = json.dumps(data, ensure_ascii=False)
            f.write(json_line + '\n')
    
    print(f"数据过滤完成！")
    print(f"- 总行数: {total_lines}")
    print(f"- {target_db_id} 数据行数: {len(filtered_lines)}")
    print(f"- 输出文件: {output_file}")

def get_available_databases(input_file: str) -> set:
    """
    获取输入文件中所有可用的数据库ID
    
    Args:
        input_file (str): 输入文件路径
        
    Returns:
        set: 所有可用的数据库ID集合
    """
    db_ids = set()
    
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                data = json.loads(line.strip())
                db_id = data.get('db_id')
                if db_id:
                    db_ids.add(db_id)
            except json.JSONDecodeError:
                continue
    
    return db_ids

def filter_by_instance_ids(input_file: str, output_file: str, target_instance_ids: list) -> None:
    """
    根据 instance_id 列表过滤 JSONL 文件
    
    Args:
        input_file (str): 输入文件路径
        output_file (str): 输出文件路径
        target_instance_ids (list): 目标 instance_id 列表
    """
    target_ids_set = set(target_instance_ids)
    filtered_lines = []
    total_lines = 0
    found_ids = set()
    
    # 读取原始文件
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            total_lines += 1
            try:
                data = json.loads(line.strip())
                instance_id = data.get('instance_id')
                if instance_id in target_ids_set:
                    filtered_lines.append(data)
                    found_ids.add(instance_id)
            except json.JSONDecodeError as e:
                print(f"警告：跳过无效的 JSON 行 (行 {total_lines}): {e}")
                continue
    
    # 写入过滤后的数据
    with open(output_file, 'w', encoding='utf-8') as f:
        for data in filtered_lines:
            json_line = json.dumps(data, ensure_ascii=False)
            f.write(json_line + '\n')
    
    # 检查缺失的 ID
    missing_ids = target_ids_set - found_ids
    
    print(f"Instance ID 过滤完成！")
    print(f"- 总行数: {total_lines}")
    print(f"- 目标 instance_id 数量: {len(target_instance_ids)}")
    print(f"- 找到的数据行数: {len(filtered_lines)}")
    print(f"- 输出文件: {output_file}")
    
    if missing_ids:
        print(f"- 未找到的 instance_id: {sorted(missing_ids)}")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='过滤 spider2-snow.jsonl 文件，只保留指定 db_id 的数据行',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
使用示例:
  python3 dataset_pre.py --db_id CRYPTO
  python3 dataset_pre.py --db_id GA360 --input custom_input.jsonl
  python3 dataset_pre.py --list-databases
  python3 dataset_pre.py --instance_ids sf_bq025 sf_bq006 sf_bq016
  python3 dataset_pre.py --instance_ids sf_bq025 sf_bq006 --output custom_output.jsonl
        '''
    )
    
    parser.add_argument('--db_id', type=str, 
                       help='要筛选的数据库ID（如：CRYPTO, GA360, PATENTS等）')
    parser.add_argument('--input', type=str, default='spider2-snow.jsonl',
                       help='输入文件路径（默认：spider2-snow.jsonl）')
    parser.add_argument('--output', type=str, 
                       help='输出文件路径（默认：spider2-snow-{db_id}.jsonl）')
    parser.add_argument('--list-databases', action='store_true',
                       help='列出输入文件中所有可用的数据库ID')
    parser.add_argument('--instance_ids', nargs='+', 
                       help='要筛选的 instance_id 列表（用空格分隔）')
    
    args = parser.parse_args()
    
    # 检查输入文件是否存在
    if not os.path.exists(args.input):
        print(f"错误：输入文件 {args.input} 不存在！")
        return
    
    # 如果用户要求列出数据库
    if args.list_databases:
        print("正在扫描可用的数据库...")
        db_ids = get_available_databases(args.input)
        print(f"\n在 {args.input} 中找到以下数据库ID：")
        for db_id in sorted(db_ids):
            print(f"  - {db_id}")
        print(f"\n总共 {len(db_ids)} 个数据库")
        return
    
    # 处理 instance_ids 过滤
    if args.instance_ids:
        # 设置输出文件名
        if args.output:
            output_file = args.output
        else:
            output_file = f"spider2-snow-instances.jsonl"
        
        # 执行基于 instance_id 的过滤
        filter_by_instance_ids(args.input, output_file, args.instance_ids)
        
        # 显示过滤后的数据样本
        print(f"\n过滤后的数据样本（Instance IDs）：")
        with open(output_file, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if i < 3:  # 显示前3行样本
                    data = json.loads(line.strip())
                    print(f"样本 {i+1}:")
                    print(f"  ID: {data.get('instance_id', 'N/A')}")
                    print(f"  指令: {data.get('instruction', 'N/A')[:100]}...")
                    print(f"  数据库: {data.get('db_id', 'N/A')}")
                    print(f"  外部知识: {data.get('external_knowledge', 'N/A')}")
                    print()
        return
    
    # 检查是否提供了数据库ID
    if not args.db_id:
        print("错误：请提供要筛选的数据库ID（使用 --db_id 参数）或 instance_id 列表（使用 --instance_ids 参数）")
        print("使用 --list-databases 查看所有可用的数据库ID")
        return
    
    # 设置输出文件名
    if args.output:
        output_file = args.output
    else:
        output_file = f"spider2-snow-{args.db_id.lower()}.jsonl"
    
    # 执行过滤
    filter_database_data(args.input, output_file, args.db_id)
    
    # 显示过滤后的数据样本
    print(f"\n过滤后的数据样本（{args.db_id}）：")
    with open(output_file, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i < 3:  # 显示前3行样本
                data = json.loads(line.strip())
                print(f"样本 {i+1}:")
                print(f"  ID: {data.get('instance_id', 'N/A')}")
                print(f"  指令: {data.get('instruction', 'N/A')[:100]}...")
                print(f"  数据库: {data.get('db_id', 'N/A')}")
                print(f"  外部知识: {data.get('external_knowledge', 'N/A')}")
                print()

if __name__ == "__main__":
    main()