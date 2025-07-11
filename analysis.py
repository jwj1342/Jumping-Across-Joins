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

def spider_analysis():
    """主函数"""
    # 默认数据文件路径
    default_path = "spider2-snow.jsonl"
    
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


class NodeAnalyzer:
    def __init__(self, enable_info_logging=False):
        """
        初始化节点分析器
        
        Args:
            enable_info_logging (bool): 是否启用详细日志
        """
        from utils.CypherExecutor import CypherExecutor
        self.executor = CypherExecutor(enable_info_logging)
        self.analysis_results = {}
        
    def verify_connection(self):
        """验证数据库连接"""
        return self.executor.verify_connectivity()
    
    def analyze_node_counts(self):
        """分析各种节点的数量"""
        print("\n=== 节点数量统计 ===")
        
        node_labels = ['Database', 'Field', 'Schema', 'SharedFieldGroup', 'Table']
        node_counts = {}
        
        for label in node_labels:
            cypher = f"MATCH (n:{label}) RETURN count(n) as count"
            success, results = self.executor.execute_transactional_cypher(cypher)
            
            if success and results:
                count = results[0]['count']
                node_counts[label] = count
                print(f"  {label}: {count}")
            else:
                print(f"  {label}: 查询失败")
                node_counts[label] = 0
        
        self.analysis_results['node_counts'] = node_counts
        return node_counts
    
    def analyze_relationship_counts(self):
        """分析各种关系的数量"""
        print("\n=== 关系数量统计 ===")
        
        relationship_types = ['HAS_FIELD', 'HAS_SCHEMA', 'HAS_TABLE', 'HAS_UNIQUE_FIELD', 'USES_FIELD_GROUP']
        relationship_counts = {}
        
        for rel_type in relationship_types:
            cypher = f"MATCH ()-[r:{rel_type}]-() RETURN count(r) as count"
            success, results = self.executor.execute_transactional_cypher(cypher)
            
            if success and results:
                count = results[0]['count']
                relationship_counts[rel_type] = count
                print(f"  {rel_type}: {count}")
            else:
                print(f"  {rel_type}: 查询失败")
                relationship_counts[rel_type] = 0
        
        self.analysis_results['relationship_counts'] = relationship_counts
        return relationship_counts
    
    def analyze_node_properties(self):
        """分析节点属性缺失情况"""
        print("\n=== 节点属性缺失分析 ===")
        
        node_labels = ['Database', 'Field', 'Schema', 'SharedFieldGroup', 'Table']
        property_analysis = {}
        
        for label in node_labels:
            print(f"\n--- {label} 节点属性分析 ---")
            
            # 获取所有属性键
            cypher = f"""
            MATCH (n:{label}) 
            UNWIND keys(n) AS key 
            RETURN DISTINCT key
            """
            success, results = self.executor.execute_transactional_cypher(cypher)
            
            if not success:
                print(f"  {label}: 获取属性键失败")
                continue
                
            properties = [result['key'] for result in results]
            print(f"  属性列表: {properties}")
            
            # 分析每个属性的缺失情况
            property_stats = {}
            total_nodes_cypher = f"MATCH (n:{label}) RETURN count(n) as total"
            success, total_results = self.executor.execute_transactional_cypher(total_nodes_cypher)
            total_nodes = total_results[0]['total'] if success and total_results else 0
            
            for prop in properties:
                cypher = f"""
                MATCH (n:{label}) 
                WHERE n.{prop} IS NOT NULL 
                RETURN count(n) as count
                """
                success, results = self.executor.execute_transactional_cypher(cypher)
                
                if success and results:
                    non_null_count = results[0]['count']
                    missing_count = total_nodes - non_null_count
                    missing_rate = (missing_count / total_nodes * 100) if total_nodes > 0 else 0
                    
                    property_stats[prop] = {
                        'total': total_nodes,
                        'non_null': non_null_count,
                        'missing': missing_count,
                        'missing_rate': missing_rate
                    }
                    
                    print(f"    {prop}: 缺失 {missing_count}/{total_nodes} ({missing_rate:.1f}%)")
            
            property_analysis[label] = property_stats
        
        self.analysis_results['property_analysis'] = property_analysis
        return property_analysis
    
    def analyze_by_database(self):
        """按数据库分组分析"""
        print("\n=== 按数据库分组分析 ===")
        
        # 获取所有数据库
        cypher = "MATCH (d:Database) RETURN d.name as db_name"
        success, results = self.executor.execute_transactional_cypher(cypher)
        
        if not success:
            print("获取数据库列表失败")
            return {}
        
        databases = [result['db_name'] for result in results if result['db_name']]
        print(f"找到 {len(databases)} 个数据库")
        
        db_analysis = {}
        
        for db_name in databases:
            print(f"\n--- 数据库: {db_name} ---")
            db_stats = {}
            
            # 分析该数据库下的各类节点数量
            # Schema
            cypher = f"""
            MATCH (d:Database {{name: '{db_name}'}})-[:HAS_SCHEMA]->(s:Schema)
            RETURN count(s) as count
            """
            success, results = self.executor.execute_transactional_cypher(cypher)
            schema_count = results[0]['count'] if success and results else 0
            db_stats['schemas'] = schema_count
            
            # Table
            cypher = f"""
            MATCH (d:Database {{name: '{db_name}'}})-[:HAS_SCHEMA]->(s:Schema)-[:HAS_TABLE]->(t:Table)
            RETURN count(t) as count
            """
            success, results = self.executor.execute_transactional_cypher(cypher)
            table_count = results[0]['count'] if success and results else 0
            db_stats['tables'] = table_count
            
            # Field - 直接通过database属性查询
            cypher = f"""
            MATCH (f:Field {{database: '{db_name}'}})
            RETURN count(f) as count
            """
            success, results = self.executor.execute_transactional_cypher(cypher)
            field_count = results[0]['count'] if success and results else 0
            db_stats['fields'] = field_count
            
            print(f"  Schemas: {schema_count}")
            print(f"  Tables: {table_count}")
            print(f"  Fields: {field_count}")
            
            db_analysis[db_name] = db_stats
        
        self.analysis_results['database_analysis'] = db_analysis
        return db_analysis
    
    def analyze_shared_field_groups(self):
        """分析SharedFieldGroup的连接情况"""
        print("\n=== SharedFieldGroup 分析 ===")
        
        # 获取所有SharedFieldGroup
        cypher = "MATCH (sfg:SharedFieldGroup) RETURN count(sfg) as total"
        success, results = self.executor.execute_transactional_cypher(cypher)
        total_sfg = results[0]['total'] if success and results else 0
        
        print(f"总共有 {total_sfg} 个 SharedFieldGroup")
        
        if total_sfg == 0:
            return {}
        
        # 分析每个SharedFieldGroup连接的表和字段数量
        cypher = """
        MATCH (sfg:SharedFieldGroup)
        OPTIONAL MATCH (t:Table)-[:USES_FIELD_GROUP]->(sfg)
        OPTIONAL MATCH (f:Field)-[:USES_FIELD_GROUP]->(sfg)
        WITH sfg, count(DISTINCT t) as table_count, count(DISTINCT f) as field_count
        RETURN 
            sfg.name as sfg_name,
            table_count,
            field_count
        ORDER BY table_count DESC, field_count DESC
        """
        
        success, results = self.executor.execute_transactional_cypher(cypher)
        
        if not success:
            print("分析SharedFieldGroup失败")
            return {}
        
        sfg_stats = []
        table_counts = []
        field_counts = []
        
        for result in results:
            sfg_name = result['sfg_name'] or '未命名'
            table_count = result['table_count']
            field_count = result['field_count']
            
            sfg_stats.append({
                'name': sfg_name,
                'tables': table_count,
                'fields': field_count
            })
            
            table_counts.append(table_count)
            field_counts.append(field_count)
        
        # 统计信息
        if table_counts:
            avg_tables = np.mean(table_counts)
            max_tables = max(table_counts)
            min_tables = min(table_counts)
            
            avg_fields = np.mean(field_counts)
            max_fields = max(field_counts)
            min_fields = min(field_counts)
            
            print(f"\n连接表数量统计:")
            print(f"  平均: {avg_tables:.1f}")
            print(f"  最多: {max_tables}")
            print(f"  最少: {min_tables}")
            
            print(f"\n连接字段数量统计:")
            print(f"  平均: {avg_fields:.1f}")
            print(f"  最多: {max_fields}")
            print(f"  最少: {min_fields}")
            
            print(f"\n前10个连接最多表的SharedFieldGroup:")
            for i, sfg in enumerate(sfg_stats[:10], 1):
                print(f"  {i}. {sfg['name']}: {sfg['tables']} 表, {sfg['fields']} 字段")
        
        sfg_analysis = {
            'total_count': total_sfg,
            'stats': sfg_stats,
            'summary': {
                'avg_tables': avg_tables if table_counts else 0,
                'max_tables': max_tables if table_counts else 0,
                'min_tables': min_tables if table_counts else 0,
                'avg_fields': avg_fields if field_counts else 0,
                'max_fields': max_fields if field_counts else 0,
                'min_fields': min_fields if field_counts else 0
            }
        }
        
        self.analysis_results['sfg_analysis'] = sfg_analysis
        return sfg_analysis
    
    def analyze_field_properties_detailed(self):
        """详细分析Field节点的属性缺失情况"""
        print("\n=== Field节点属性详细分析 ===")
        
        # 获取所有Field节点的总数
        cypher = "MATCH (f:Field) RETURN count(f) as total"
        success, results = self.executor.execute_transactional_cypher(cypher)
        total_fields = results[0]['total'] if success and results else 0
        
        print(f"总Field节点数: {total_fields}")
        
        if total_fields == 0:
            print("没有找到Field节点")
            return {}
        
        # 分析Field节点的各个属性
        field_properties = ['database', 'description', 'name', 'node_type', 'sample_data', 'schema', 'table', 'type']
        property_analysis = {}
        
        print(f"\nField节点属性缺失统计:")
        for prop in field_properties:
            cypher = f"""
            MATCH (f:Field) 
            WHERE f.{prop} IS NOT NULL AND f.{prop} <> ''
            RETURN count(f) as count
            """
            success, results = self.executor.execute_transactional_cypher(cypher)
            
            if success and results:
                non_null_count = results[0]['count']
                missing_count = total_fields - non_null_count
                missing_rate = (missing_count / total_fields * 100) if total_fields > 0 else 0
                
                property_analysis[prop] = {
                    'total': total_fields,
                    'non_null': non_null_count,
                    'missing': missing_count,
                    'missing_rate': missing_rate
                }
                
                print(f"  {prop}: 有值 {non_null_count}/{total_fields}, 缺失 {missing_count} ({missing_rate:.1f}%)")
        
        self.analysis_results['field_property_analysis'] = property_analysis
        return property_analysis
    
    def analyze_description_by_database(self):
        """按数据库分析description字段缺失情况"""
        print("\n=== 按数据库分析description缺失情况 ===")
        
        cypher = """
        MATCH (f:Field)
        WHERE f.database IS NOT NULL
        WITH f.database as db_name, 
             count(f) as total_fields,
             sum(CASE WHEN f.description IS NULL OR f.description = '' THEN 1 ELSE 0 END) as missing_desc,
             sum(CASE WHEN f.description IS NOT NULL AND f.description <> '' THEN 1 ELSE 0 END) as has_desc
        RETURN db_name, total_fields, missing_desc, has_desc,
               (missing_desc * 100.0 / total_fields) as missing_rate
        ORDER BY missing_rate DESC, total_fields DESC
        """
        
        success, results = self.executor.execute_transactional_cypher(cypher)
        
        if not success:
            print("查询失败")
            return {}
        
        desc_analysis = {}
        
        print(f"按description缺失率排序的数据库:")
        print(f"{'数据库名称':<40} {'总字段数':<10} {'缺失数':<8} {'有值数':<8} {'缺失率':<8}")
        print("-" * 80)
        
        for result in results:
            db_name = result['db_name']
            total_fields = result['total_fields']
            missing_desc = result['missing_desc']
            has_desc = result['has_desc']
            missing_rate = result['missing_rate']
            
            desc_analysis[db_name] = {
                'total_fields': total_fields,
                'missing_desc': missing_desc,
                'has_desc': has_desc,
                'missing_rate': missing_rate
            }
            
            print(f"{db_name:<40} {total_fields:<10} {missing_desc:<8} {has_desc:<8} {missing_rate:<7.1f}%")
        
        # 统计摘要
        if results:
            total_all_fields = sum(r['total_fields'] for r in results)
            total_missing = sum(r['missing_desc'] for r in results)
            overall_missing_rate = (total_missing / total_all_fields * 100) if total_all_fields > 0 else 0
            
            print(f"\n整体统计:")
            print(f"  总字段数: {total_all_fields}")
            print(f"  总缺失数: {total_missing}")
            print(f"  整体缺失率: {overall_missing_rate:.1f}%")
            
            # 找出缺失最多的数据库
            max_missing_db = max(results, key=lambda x: x['missing_desc'])
            print(f"  缺失最多的数据库: {max_missing_db['db_name']} (缺失 {max_missing_db['missing_desc']} 个)")
            
            # 找出缺失率最高的数据库
            max_rate_db = max(results, key=lambda x: x['missing_rate'])
            print(f"  缺失率最高的数据库: {max_rate_db['db_name']} ({max_rate_db['missing_rate']:.1f}%)")
        
        self.analysis_results['description_analysis'] = desc_analysis
        return desc_analysis
    
    def analyze_field_types_distribution(self):
        """分析Field节点的type字段分布"""
        print("\n=== Field类型分布分析 ===")
        
        cypher = """
        MATCH (f:Field)
        WHERE f.type IS NOT NULL AND f.type <> ''
        WITH f.type as field_type, count(f) as type_count
        RETURN field_type, type_count
        ORDER BY type_count DESC
        LIMIT 20
        """
        
        success, results = self.executor.execute_transactional_cypher(cypher)
        
        if not success:
            print("查询Field类型分布失败")
            return {}
        
        print(f"前20种最常见的Field类型:")
        print(f"{'类型':<30} {'数量':<10} {'占比':<8}")
        print("-" * 50)
        
        total_typed_fields = sum(r['type_count'] for r in results)
        type_distribution = {}
        
        for result in results:
            field_type = result['field_type']
            type_count = result['type_count']
            percentage = (type_count / total_typed_fields * 100) if total_typed_fields > 0 else 0
            
            type_distribution[field_type] = {
                'count': type_count,
                'percentage': percentage
            }
            
            print(f"{field_type:<30} {type_count:<10} {percentage:<7.1f}%")
        
        self.analysis_results['field_type_distribution'] = type_distribution
        return type_distribution
    
    def analyze_node_type_distribution(self):
        """分析Field节点的node_type字段分布"""
        print("\n=== Field node_type分布分析 ===")
        
        cypher = """
        MATCH (f:Field)
        WHERE f.node_type IS NOT NULL AND f.node_type <> ''
        WITH f.node_type as node_type, count(f) as count
        RETURN node_type, count
        ORDER BY count DESC
        """
        
        success, results = self.executor.execute_transactional_cypher(cypher)
        
        if not success:
            print("查询Field node_type分布失败")
            return {}
        
        print(f"Field node_type分布:")
        print(f"{'Node Type':<20} {'数量':<10} {'占比':<8}")
        print("-" * 40)
        
        total_node_typed_fields = sum(r['count'] for r in results)
        node_type_distribution = {}
        
        for result in results:
            node_type = result['node_type']
            count = result['count']
            percentage = (count / total_node_typed_fields * 100) if total_node_typed_fields > 0 else 0
            
            node_type_distribution[node_type] = {
                'count': count,
                'percentage': percentage
            }
            
            print(f"{node_type:<20} {count:<10} {percentage:<7.1f}%")
        
        self.analysis_results['node_type_distribution'] = node_type_distribution
        return node_type_distribution
    
    def analyze_database_schema_structure(self):
        """分析数据库的Schema结构"""
        print("\n=== 数据库Schema结构分析 ===")
        
        cypher = """
        MATCH (d:Database)-[:HAS_SCHEMA]->(s:Schema)
        OPTIONAL MATCH (s)-[:HAS_TABLE]->(t:Table)
        OPTIONAL MATCH (t)-[:HAS_FIELD]->(f:Field)
        WITH d, s, count(DISTINCT t) as table_count, count(DISTINCT f) as field_count
        RETURN 
            d.name as db_name,
            s.name as schema_name,
            table_count,
            field_count
        ORDER BY d.name, table_count DESC
        """
        
        success, results = self.executor.execute_transactional_cypher(cypher)
        
        if not success:
            print("分析数据库Schema结构失败")
            return {}
        
        schema_structure = {}
        
        for result in results:
            db_name = result['db_name'] or '未知数据库'
            schema_name = result['schema_name'] or '未知Schema'
            table_count = result['table_count']
            field_count = result['field_count']
            
            if db_name not in schema_structure:
                schema_structure[db_name] = []
            
            schema_structure[db_name].append({
                'schema': schema_name,
                'tables': table_count,
                'fields': field_count
            })
        
        for db_name, schemas in schema_structure.items():
            print(f"\n数据库: {db_name}")
            total_tables = sum(s['tables'] for s in schemas)
            total_fields = sum(s['fields'] for s in schemas)
            print(f"  总Schema数: {len(schemas)}")
            print(f"  总表数: {total_tables}")
            print(f"  总字段数: {total_fields}")
            
            print(f"  Schema详情:")
            for schema in schemas:
                print(f"    {schema['schema']}: {schema['tables']} 表, {schema['fields']} 字段")
        
        self.analysis_results['schema_structure'] = schema_structure
        return schema_structure
    
    def generate_comprehensive_report(self):
        """生成综合分析报告"""
        print("\n" + "="*60)
        print("             Neo4j 图数据库综合分析报告")
        print("="*60)
        
        # 验证连接
        if not self.verify_connection():
            print("错误：无法连接到Neo4j数据库")
            return
        
        # 执行所有分析
        node_counts = self.analyze_node_counts()
        relationship_counts = self.analyze_relationship_counts()
        property_analysis = self.analyze_node_properties()
        database_analysis = self.analyze_by_database()
        sfg_analysis = self.analyze_shared_field_groups()
        field_property_analysis = self.analyze_field_properties_detailed()
        description_analysis = self.analyze_description_by_database()
        field_type_distribution = self.analyze_field_types_distribution()
        node_type_distribution = self.analyze_node_type_distribution()
        schema_structure = self.analyze_database_schema_structure()
        
        # 生成报告文件
        report_path = "neo4j_analysis_report.txt"
        try:
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write("Neo4j 图数据库分析报告\n")
                f.write("="*60 + "\n\n")
                
                # 节点统计
                f.write("节点数量统计:\n")
                total_nodes = sum(node_counts.values())
                f.write(f"  总节点数: {total_nodes}\n")
                for label, count in node_counts.items():
                    percentage = (count / total_nodes * 100) if total_nodes > 0 else 0
                    f.write(f"  {label}: {count} ({percentage:.1f}%)\n")
                
                # 关系统计
                f.write(f"\n关系数量统计:\n")
                total_relationships = sum(relationship_counts.values())
                f.write(f"  总关系数: {total_relationships}\n")
                for rel_type, count in relationship_counts.items():
                    f.write(f"  {rel_type}: {count}\n")
                
                # 数据库分析
                f.write(f"\n数据库分析:\n")
                f.write(f"  数据库总数: {len(database_analysis)}\n")
                for db_name, stats in database_analysis.items():
                    f.write(f"  {db_name}: {stats['schemas']} schemas, {stats['tables']} tables, {stats['fields']} fields\n")
                
                # SharedFieldGroup分析
                if 'sfg_analysis' in self.analysis_results:
                    sfg = self.analysis_results['sfg_analysis']
                    f.write(f"\nSharedFieldGroup分析:\n")
                    f.write(f"  总数: {sfg['total_count']}\n")
                    if sfg['summary']:
                        f.write(f"  平均连接表数: {sfg['summary']['avg_tables']:.1f}\n")
                        f.write(f"  平均连接字段数: {sfg['summary']['avg_fields']:.1f}\n")
                        f.write(f"  最多连接表数: {sfg['summary']['max_tables']}\n")
                        f.write(f"  最少连接表数: {sfg['summary']['min_tables']}\n")
                
                # Field属性分析
                if 'field_property_analysis' in self.analysis_results:
                    field_props = self.analysis_results['field_property_analysis']
                    f.write(f"\nField属性缺失分析:\n")
                    for prop, stats in field_props.items():
                        f.write(f"  {prop}: 缺失率 {stats['missing_rate']:.1f}% ({stats['missing']}/{stats['total']})\n")
                
                # Description缺失分析
                if 'description_analysis' in self.analysis_results:
                    desc_analysis = self.analysis_results['description_analysis']
                    f.write(f"\nDescription缺失分析 (前10个缺失最多的数据库):\n")
                    sorted_desc = sorted(desc_analysis.items(), key=lambda x: x[1]['missing_desc'], reverse=True)
                    for i, (db_name, stats) in enumerate(sorted_desc[:10], 1):
                        f.write(f"  {i}. {db_name}: 缺失 {stats['missing_desc']}/{stats['total_fields']} ({stats['missing_rate']:.1f}%)\n")
                
                # Field类型分布
                if 'field_type_distribution' in self.analysis_results:
                    type_dist = self.analysis_results['field_type_distribution']
                    f.write(f"\n最常见的Field类型 (前5名):\n")
                    sorted_types = sorted(type_dist.items(), key=lambda x: x[1]['count'], reverse=True)
                    for i, (field_type, stats) in enumerate(sorted_types[:5], 1):
                        f.write(f"  {i}. {field_type}: {stats['count']} ({stats['percentage']:.1f}%)\n")
                
                # Node类型分布
                if 'node_type_distribution' in self.analysis_results:
                    node_dist = self.analysis_results['node_type_distribution']
                    f.write(f"\nField node_type分布:\n")
                    for node_type, stats in node_dist.items():
                        f.write(f"  {node_type}: {stats['count']} ({stats['percentage']:.1f}%)\n")
            
            print(f"\n详细报告已保存至: {report_path}")
            
        except Exception as e:
            print(f"保存报告失败: {e}")
        
        print("\nNeo4j图数据库分析完成！")
    
    def close(self):
        """关闭数据库连接"""
        if self.executor:
            self.executor.close()


def main():
    """主函数"""
    print("选择分析类型:")
    print("1. JSONL数据集分析")
    print("2. Neo4j图数据库分析")
    
    choice = input("请输入选择 (1 或 2): ").strip()
    
    if choice == "1":
        spider_analysis()
    elif choice == "2":
        print("\n开始Neo4j图数据库分析...")
        analyzer = NodeAnalyzer(enable_info_logging=True)
        try:
            analyzer.generate_comprehensive_report()
        finally:
            analyzer.close()
    else:
        print("无效选择，默认执行JSONL数据集分析")
        spider_analysis()


if __name__ == "__main__":
    # main()
    spider_analysis()
