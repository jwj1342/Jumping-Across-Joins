import json
from collections import Counter
import pandas as pd
import numpy as np
from pathlib import Path

class DatasetAnalyzer:
    def __init__(self, jsonl_path):
        """
        初始化数据集分析器
        
        Args:
            jsonl_path (str): JSONL文件路径
        """
        self.jsonl_path = jsonl_path
        self.data = []
        self.load_data()
    
    def load_data(self):
        """加载JSONL数据"""
        try:
            with open(self.jsonl_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        self.data.append(json.loads(line))
            print(f"成功加载 {len(self.data)} 条数据")
        except FileNotFoundError:
            print(f"错误：找不到文件 {self.jsonl_path}")
        except json.JSONDecodeError as e:
            print(f"错误：JSON解析失败 - {e}")
    
    def analyze_database_frequency(self):
        """分析数据库使用频率"""
        print("\n=== 数据库使用频率分析 ===")
        
        # 统计db_id频率
        db_ids = [item.get('db_id', 'Unknown') for item in self.data]
        db_counter = Counter(db_ids)
        
        print(f"总共涉及 {len(db_counter)} 个不同的数据库")
        print("\n数据库使用频率排序:")
        for db_id, count in db_counter.most_common():
            percentage = (count / len(self.data)) * 100
            print(f"  {db_id}: {count} 次 ({percentage:.1f}%)")
        
        return db_counter
    
    def analyze_external_knowledge(self):
        """分析外部知识文档使用情况"""
        print("\n=== 外部知识文档使用分析 ===")
        
        # 统计external_knowledge字段
        external_docs = []
        no_external = 0
        
        for item in self.data:
            ext_knowledge = item.get('external_knowledge', '')
            if ext_knowledge and ext_knowledge.strip():
                external_docs.append(ext_knowledge)
            else:
                no_external += 1
        
        print(f"使用外部文档的记录: {len(external_docs)} 条 ({len(external_docs)/len(self.data)*100:.1f}%)")
        print(f"未使用外部文档的记录: {no_external} 条 ({no_external/len(self.data)*100:.1f}%)")
        
        if external_docs:
            # 统计外部文档频率
            doc_counter = Counter(external_docs)
            print(f"\n涉及 {len(doc_counter)} 个不同的外部文档")
            print("\n外部文档使用频率排序:")
            for doc, count in doc_counter.most_common():
                percentage = (count / len(external_docs)) * 100
                print(f"  {doc}: {count} 次 ({percentage:.1f}%)")
            
            # 分析文档类型
            doc_types = {}
            for doc in external_docs:
                if '.md' in doc:
                    doc_types['Markdown'] = doc_types.get('Markdown', 0) + 1
                elif '.txt' in doc:
                    doc_types['Text'] = doc_types.get('Text', 0) + 1
                elif '.json' in doc:
                    doc_types['JSON'] = doc_types.get('JSON', 0) + 1
                else:
                    doc_types['Other'] = doc_types.get('Other', 0) + 1
            
            print(f"\n文档类型分布:")
            for doc_type, count in doc_types.items():
                percentage = (count / len(external_docs)) * 100
                print(f"  {doc_type}: {count} 次 ({percentage:.1f}%)")
        
        return external_docs, doc_counter if external_docs else Counter()
    
    def analyze_query_length(self):
        """分析查询长度"""
        print("\n=== 查询长度分析 ===")
        
        # 提取instruction字段并计算长度
        instructions = [item.get('instruction', '') for item in self.data]
        char_lengths = [len(instruction) for instruction in instructions]
        word_lengths = [len(instruction.split()) for instruction in instructions]
        
        # 统计信息
        print(f"查询数量: {len(instructions)}")
        print(f"\n字符长度统计:")
        print(f"  平均长度: {np.mean(char_lengths):.1f} 字符")
        print(f"  中位数长度: {np.median(char_lengths):.1f} 字符")
        print(f"  最短长度: {min(char_lengths)} 字符")
        print(f"  最长长度: {max(char_lengths)} 字符")
        print(f"  标准差: {np.std(char_lengths):.1f}")
        
        print(f"\n单词长度统计:")
        print(f"  平均长度: {np.mean(word_lengths):.1f} 单词")
        print(f"  中位数长度: {np.median(word_lengths):.1f} 单词")
        print(f"  最短长度: {min(word_lengths)} 单词")
        print(f"  最长长度: {max(word_lengths)} 单词")
        print(f"  标准差: {np.std(word_lengths):.1f}")
        
        # 长度分布区间
        char_bins = [0, 50, 100, 150, 200, 250, 300, float('inf')]
        char_labels = ['0-50', '51-100', '101-150', '151-200', '201-250', '251-300', '300+']
        char_distribution = pd.cut(char_lengths, bins=char_bins, labels=char_labels, right=False)
        
        word_bins = [0, 10, 20, 30, 40, 50, float('inf')]
        word_labels = ['0-10', '11-20', '21-30', '31-40', '41-50', '50+']
        word_distribution = pd.cut(word_lengths, bins=word_bins, labels=word_labels, right=False)
        
        print(f"\n字符长度分布:")
        char_dist_counts = char_distribution.value_counts().sort_index()
        for label, count in char_dist_counts.items():
            percentage = (count / len(char_lengths)) * 100
            print(f"  {label}: {count} 条 ({percentage:.1f}%)")
        
        print(f"\n单词长度分布:")
        word_dist_counts = word_distribution.value_counts().sort_index()
        for label, count in word_dist_counts.items():
            percentage = (count / len(word_lengths)) * 100
            print(f"  {label}: {count} 条 ({percentage:.1f}%)")
        
        return char_lengths, word_lengths
    
    def analyze_instance_ids(self):
        """分析实例ID模式"""
        print("\n=== 实例ID模式分析 ===")
        
        instance_ids = [item.get('instance_id', '') for item in self.data]
        
        # 分析ID前缀
        id_prefixes = {}
        for instance_id in instance_ids:
            if instance_id:
                prefix = instance_id.split('_')[0] if '_' in instance_id else instance_id
                id_prefixes[prefix] = id_prefixes.get(prefix, 0) + 1
        
        print(f"实例ID前缀分布:")
        for prefix, count in sorted(id_prefixes.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / len(instance_ids)) * 100
            print(f"  {prefix}: {count} 次 ({percentage:.1f}%)")
        
        # 分析ID编号范围
        print(f"\n实例ID编号分析:")
        for prefix in id_prefixes:
            nums = []
            for instance_id in instance_ids:
                if instance_id.startswith(prefix + '_'):
                    try:
                        num = int(instance_id.split('_')[1])
                        nums.append(num)
                    except (ValueError, IndexError):
                        continue
            
            if nums:
                print(f"  {prefix} 编号范围: {min(nums)} - {max(nums)} (共 {len(nums)} 个)")
    
    def generate_summary_report(self):
        """生成综合分析报告"""
        print("\n" + "="*50)
        print("             综合分析报告")
        print("="*50)
        
        # 基本信息
        print(f"数据集总记录数: {len(self.data)}")
        
        # 数据库分析
        db_counter = self.analyze_database_frequency()
        
        # 外部文档分析
        external_docs, doc_counter = self.analyze_external_knowledge()
        
        # 查询长度分析
        char_lengths, word_lengths = self.analyze_query_length()
        
        # 实例ID分析
        self.analyze_instance_ids()
        
        # 生成报告文件
        report_path = "analysis_report.txt"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("数据集分析报告\n")
            f.write("="*50 + "\n\n")
            
            f.write(f"基本信息:\n")
            f.write(f"  总记录数: {len(self.data)}\n")
            f.write(f"  涉及数据库数: {len(db_counter)}\n")
            f.write(f"  外部文档使用率: {len(external_docs)/len(self.data)*100:.1f}%\n\n")
            
            f.write("数据库使用频率:\n")
            for db_id, count in db_counter.most_common():
                percentage = (count / len(self.data)) * 100
                f.write(f"  {db_id}: {count} 次 ({percentage:.1f}%)\n")
            
            f.write(f"\n外部文档使用频率:\n")
            for doc, count in doc_counter.most_common():
                percentage = (count / len(external_docs)) * 100 if external_docs else 0
                f.write(f"  {doc}: {count} 次 ({percentage:.1f}%)\n")
            
            f.write(f"\n查询长度统计:\n")
            f.write(f"  平均字符长度: {np.mean(char_lengths):.1f}\n")
            f.write(f"  平均单词长度: {np.mean(word_lengths):.1f}\n")
            f.write(f"  字符长度范围: {min(char_lengths)} - {max(char_lengths)}\n")
            f.write(f"  单词长度范围: {min(word_lengths)} - {max(word_lengths)}\n")
        
        print(f"\n详细报告已保存至: {report_path}")
        print("\n分析完成！")

def main():
    """主函数"""
    # 默认数据文件路径
    default_path = "baseline/spider2-snow.jsonl"
    
    print("欢迎使用数据集分析工具!")
    print(f"默认分析文件: {default_path}")
    
    # 检查文件是否存在
    if not Path(default_path).exists():
        print(f"错误: 找不到文件 {default_path}")
        custom_path = input("请输入JSONL文件路径: ")
        if custom_path:
            default_path = custom_path
    
    # 创建分析器并执行分析
    analyzer = DatasetAnalyzer(default_path)
    
    if analyzer.data:
        analyzer.generate_summary_report()
    else:
        print("无法加载数据，请检查文件路径和格式")

if __name__ == "__main__":
    main()
