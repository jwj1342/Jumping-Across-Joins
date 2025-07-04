"""
图构建辅助工具模块
提供图构建过程中需要的各种辅助功能
"""
import os
import csv
import hashlib
import re
import logging
from typing import Dict, List, Optional

# 创建带有模块名的logger
logger = logging.getLogger(__name__)


class GraphUtils:
    """图构建辅助工具类，提供各种工具方法"""
    
    @staticmethod
    def calculate_field_group_hash(column_names: List[str], column_types: List[str]) -> str:
        """计算字段组的哈希值，用于识别相同字段组合"""
        # 创建字段组字符串：字段名:类型的组合
        field_items = []
        for i, name in enumerate(column_names):
            col_type = column_types[i] if i < len(column_types) else "UNKNOWN"
            field_items.append(f"{name}:{col_type}")
        
        field_str = "|".join(sorted(field_items))  # 排序确保一致性
        return hashlib.md5(field_str.encode()).hexdigest()
    
    @staticmethod
    def generate_field_group_name(representative_table: str, schema_name: str, field_count: int) -> str:
        """生成字段组名称"""
        # 移除schema前缀
        base_name = representative_table.replace(f"{schema_name}.", "")
        
        # 常见的时间/版本模式
        patterns = [
            r'_\d{4}_Q\d$',        # _1998_Q1
            r'_\d{4}$',            # _2020
            r'_\d{6}$',            # _202012
            r'_\d{8}$',            # _20201231
            r'_v\d+$',             # _v1, _v2
            r'_\d+$',              # _1, _2, _3
        ]
        
        group_base = base_name
        for pattern in patterns:
            group_base = re.sub(pattern, '', group_base)
        
        return f"{schema_name}.{group_base}_FieldGroup_{field_count}F"
    
    @staticmethod
    def load_ddl_info(ddl_file_path: str) -> Dict[str, str]:
        """加载DDL信息"""
        ddl_info = {}
        if not os.path.exists(ddl_file_path):
            return ddl_info
            
        try:
            with open(ddl_file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    table_name = row.get('table_name', '')
                    ddl = row.get('DDL', '')
                    if table_name:
                        ddl_info[table_name] = ddl
        except Exception as e:
            logger.error(f"GraphUtils: 加载DDL文件失败: {e}")
        
        return ddl_info
    
    @staticmethod
    def extract_sample_data(sample_rows: List[Dict], column_name: str, max_samples: int = 3) -> str:
        """提取示例数据"""
        samples = []
        for row in sample_rows[:max_samples]:
            if column_name in row and row[column_name] is not None:
                value = str(row[column_name])
                if value and value != "NULL":
                    # 清理和限制样本数据长度，避免特殊字符问题
                    clean_value = value.replace("'", "").replace('"', "").replace("\n", " ").replace("\r", "")
                    if len(clean_value) > 20:
                        clean_value = clean_value[:20] + "..."
                    samples.append(clean_value)
        
        return ", ".join(samples) if samples else ""
    
    @staticmethod
    def find_field_in_shared_groups(field_name: str, field_type: str, schema_name: str, 
                                  field_groups: Dict[str, Dict]) -> Optional[str]:
        """查找字段是否属于某个共享字段组，返回字段组名称（如果存在）"""
        for field_hash, group_info in field_groups.items():
            if group_info['schema'] == schema_name:
                # 检查字段是否在这个字段组中
                column_names = group_info['column_names']
                column_types = group_info['column_types']
                
                for i, col_name in enumerate(column_names):
                    col_type = column_types[i] if i < len(column_types) else "UNKNOWN"
                    if col_name == field_name and col_type == field_type:
                        return group_info['group_name']
        return None 