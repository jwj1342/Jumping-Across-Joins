# import json
# import os

# # 读取instance_results.json文件
# with open('method/batch_results_20250714_163822/instance_results.json', 'r') as f:
#     results = json.load(f)

# # 提取所有status不为success_with_data的instance_id
# no_data_ids = [item['instance_id'] for item in results if item['status'] != 'success_with_data']

# # 读取原始instances文件并筛选
# output_instances = []
# with open('method/spider2-snow-instances.jsonl', 'r') as f:
#     for line in f:
#         instance = json.loads(line.strip())
#         if instance['instance_id'] in no_data_ids:
#             output_instances.append(instance)

# # 写入新文件
# output_file = 'method/spider2-snow-instances-failed.jsonl'
# with open(output_file, 'w') as f:
#     for instance in output_instances:
#         f.write(json.dumps(instance) + '\n')

# print(f"处理完成！找到 {len(no_data_ids)} 个失败实例。")


# ---------------------------------------------------------------------

# import json
# import csv

# def export_instances_by_status(json_file_path, output_csv_path, target_status='success_with_data'):
#     """
#     从JSON文件中读取数据，根据指定的status导出instance_id到CSV文件
    
#     Args:
#         json_file_path (str): 输入JSON文件路径
#         output_csv_path (str): 输出CSV文件路径
#         target_status (str): 目标status状态，默认为'success_with_data'
#     """
#     # 读取JSON文件
#     with open(json_file_path, 'r') as f:
#         results = json.load(f)
    
#     # 筛选指定status的instance_id
#     filtered_ids = [item['instance_id'] for item in results if item['status'] == target_status]
    
#     # 写入CSV文件
#     with open(output_csv_path, 'w', newline='') as f:
#         writer = csv.writer(f)
#         writer.writerow(['instance_id'])  # 写入表头
#         for instance_id in filtered_ids:
#             writer.writerow([instance_id])
    
#     print(f"处理完成！找到 {len(filtered_ids)} 个 {target_status} 状态的实例。")
#     print(f"结果已保存到: {output_csv_path}")

# # 使用示例
# if __name__ == "__main__":
#     json_file = 'method/batch_results_20250717_220236/instance_results.json'
#     output_csv = 'success_instances.csv'
#     export_instances_by_status(json_file, output_csv, 'success_with_data')

# ---------------------------------------------------------------------

import json

def filter_instances_by_list():
    """
    根据指定的instance_id列表筛选jsonl文件中的实例
    """
    # 指定的instance_id列表
    selected_ids = [
        'sf035',
        'sf_local284',
        'sf_local058',
        'sf_local041',
        'sf_local039',
        'sf_local031',
        'sf_local004',
        'sf_bq375',
        'sf_bq341',
        'sf_bq300',
        'sf_bq285',
        'sf_bq284',
        'sf_bq280',
        'sf_bq232',
        'sf_bq228',
        'sf_bq210',
        'sf_bq198',
        'sf_bq172',
        'sf_bq130',
        'sf_bq115',
        'sf_bq077',
        'sf_bq060',
        'sf_bq025'
    ]
    
    # 转换为集合以提高查找效率
    selected_ids_set = set(selected_ids)
    
    # 读取原始jsonl文件并筛选
    output_instances = []
    input_file = 'spider2-snow.jsonl'
    
    with open(input_file, 'r') as f:
        for line in f:
            instance = json.loads(line.strip())
            if instance['instance_id'] in selected_ids_set:
                output_instances.append(instance)
    
    # 写入新的jsonl文件
    output_file = 'method/spider2-snow-instances-select.jsonl'
    with open(output_file, 'w') as f:
        for instance in output_instances:
            f.write(json.dumps(instance) + '\n')
    
    print(f"筛选完成！从 {len(selected_ids)} 个指定ID中找到 {len(output_instances)} 个匹配实例。")
    print(f"结果已保存到: {output_file}")

# 执行筛选
if __name__ == "__main__":
    filter_instances_by_list()