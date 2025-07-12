import json
import os

# 读取instance_results.json文件
with open('method/batch_results_20250712_184544/instance_results.json', 'r') as f:
    results = json.load(f)

# 提取所有status为success_no_data的instance_id
no_data_ids = [item['instance_id'] for item in results if item['status'] == 'success_no_data']

# 读取原始instances文件并筛选
output_instances = []
with open('method/spider2-snow-instances.jsonl', 'r') as f:
    for line in f:
        instance = json.loads(line.strip())
        if instance['instance_id'] in no_data_ids:
            output_instances.append(instance)

# 写入新文件
output_file = 'method/spider2-snow-instances-nodata.jsonl'
with open(output_file, 'w') as f:
    for instance in output_instances:
        f.write(json.dumps(instance) + '\n')

print(f"处理完成！找到 {len(no_data_ids)} 个no_data实例。")
