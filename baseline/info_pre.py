import os
import json
import csv
from pathlib import Path

def extract_database_info(include_samples=True):
    """
    提取resource/databases下所有数据库的结构信息
    生成每个数据库的描述文件到baseline/db_info目录
    
    Args:
        include_samples (bool): 是否包含示例数据，默认为True
    """
    # 设置路径
    databases_path = Path("resource/databases")
    output_path = Path("baseline/db_info")
    
    # 创建输出目录
    output_path.mkdir(exist_ok=True)
    
    # 遍历所有数据库文件夹
    for db_folder in databases_path.iterdir():
        if not db_folder.is_dir():
            continue
            
        db_name = db_folder.name
        print(f"处理数据库: {db_name}")
        
        # 查找schema文件夹（通常与数据库同名）
        schema_folders = [f for f in db_folder.iterdir() if f.is_dir()]
        
        if not schema_folders:
            print(f"  跳过: {db_name} (无schema文件夹)")
            continue
        
        # 生成数据库描述
        db_description = generate_database_description(db_name, db_folder, schema_folders, include_samples)
        
        # 写入描述文件
        output_file = output_path / f"{db_name}.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(db_description.strip())
        
        print(f"  完成: {output_file}")

def generate_database_description(db_name, db_folder, schema_folders, include_samples=True):
    """
    生成单个数据库的描述信息
    """
    description = f"DATABASE:{db_name}\n"
    
    for schema_folder in schema_folders:
        schema_name = schema_folder.name
        description += f"SCHEMA:{schema_name}\n"
        
        # 读取DDL.csv文件获取表结构
        ddl_file = schema_folder / "DDL.csv"
        tables_info = {}
        
        if ddl_file.exists():
            tables_info = parse_ddl_csv(ddl_file)
        
        # 遍历JSON文件获取详细字段信息
        json_files = [f for f in schema_folder.glob("*.json")]
        
        for json_file in json_files:
            table_name = json_file.stem
            
            # 跳过非表JSON文件（如果有的话）
            if table_name == "DDL":
                continue
                
            description += generate_table_description(table_name, json_file, tables_info.get(table_name, {}), include_samples)
    
    return description

def parse_ddl_csv(ddl_file):
    """
    解析DDL.csv文件，提取表结构信息
    """
    tables_info = {}
    
    try:
        with open(ddl_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                table_name = row['table_name']
                tables_info[table_name] = {
                    'description': row.get('description', ''),
                    'ddl': row.get('DDL', '')
                }
    except Exception as e:
        print(f"  DDL解析失败: {e}")
    
    return tables_info

def generate_table_description(table_name, json_file, ddl_info, include_samples=True):
    """
    生成单个表的描述信息
    """
    description = f"TABLE:{table_name}\n"
    
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            table_data = json.load(f)
        
        # 字段信息
        column_names = table_data.get('column_names', [])
        column_types = table_data.get('column_types', [])
        column_descriptions = table_data.get('description', [])
        
        if column_names:
            description += "COLUMNS:"
            for i, col_name in enumerate(column_names):
                col_type = column_types[i] if i < len(column_types) else 'UNKNOWN'
                col_desc = column_descriptions[i] if i < len(column_descriptions) and column_descriptions[i] else ''
                
                description += f"{col_name}({col_type})"
                if col_desc:
                    description += f"[{col_desc}]"
                if i < len(column_names) - 1:
                    description += "|"
            description += "\n"
        
        # 示例数据
        if include_samples:
            sample_rows = table_data.get('sample_rows', [])
            if sample_rows:
                description += "SAMPLES:\n"
                
                # 显示前2行示例数据
                max_rows = min(1, len(sample_rows))
                for idx, row in enumerate(sample_rows[:max_rows]):
                    values = []
                    for col_name in column_names:
                        if col_name in row:
                            value = str(row[col_name]).replace('\n', ' ').replace('\r', ' ')
                            values.append(f"{col_name}:{value}")
                    description += f"{idx+1}|{' '.join(values)}\n"
    
    except Exception as e:
        description += f"ERROR:{e}\n"
    
    return description

if __name__ == "__main__":
    print("开始提取数据库信息...")
    
    # 默认包含示例数据
    include_samples = True
    
    # 如果不需要示例数据，可以设置为False
    # include_samples = False
    
    extract_database_info(include_samples=include_samples)
    print("完成!")
