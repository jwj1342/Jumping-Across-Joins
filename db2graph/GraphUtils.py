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
from collections import defaultdict

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
    def generate_field_group_name(representative_table: str, schema_name: str, field_count: int, field_hash: str) -> str:
        """生成字段组名称（使用字段组哈希确保唯一性）"""
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
        
        # 使用字段组哈希的前8位确保唯一性
        hash_suffix = field_hash[:8]
        return f"{schema_name}.{group_base}_FieldGroup_{field_count}F_{hash_suffix}"
    
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
    def find_exact_matching_field_group(table_fields: List[tuple], schema_name: str, 
                                      field_groups: Dict[str, Dict]) -> Optional[str]:
        """
        查找与表字段集合完全匹配的字段组（精确匹配）
        Args:
            table_fields: [(field_name, field_type), ...] 表的字段列表
            schema_name: 模式名称
            field_groups: 字段组信息
        Returns:
            匹配的字段组名称，如果没有精确匹配则返回None
        """
        # 构建表的字段集合
        table_field_set = set(f"{name}:{type_}" for name, type_ in table_fields)
        
        logger.debug(f"GraphUtils: 查找表字段集合的精确匹配字段组")
        logger.debug(f"GraphUtils:   表字段集合: {sorted(table_field_set)}")
        
        for field_hash, group_info in field_groups.items():
            if group_info['schema'] == schema_name:
                # 构建字段组的字段集合
                column_names = group_info['column_names']
                column_types = group_info['column_types']
                
                group_field_set = set()
                for i, col_name in enumerate(column_names):
                    col_type = column_types[i] if i < len(column_types) else "UNKNOWN"
                    group_field_set.add(f"{col_name}:{col_type}")
                
                logger.debug(f"GraphUtils:   检查字段组 {group_info['group_name']} (哈希: {field_hash[:8]}...)")
                logger.debug(f"GraphUtils:     组字段集合: {sorted(group_field_set)}")
                
                # 精确匹配：字段集合必须完全相同
                if table_field_set == group_field_set:
                    logger.debug(f"GraphUtils:     ✓ 精确匹配到字段组: {group_info['group_name']}")
                    return group_info['group_name']
                else:
                    logger.debug(f"GraphUtils:     ✗ 字段集合不完全匹配")
        
        logger.debug(f"GraphUtils: 未找到精确匹配的字段组")
        return None
    
    @staticmethod
    def find_field_in_shared_groups(field_name: str, field_type: str, schema_name: str, 
                                  field_groups: Dict[str, Dict]) -> Optional[str]:
        """查找字段是否属于某个共享字段组，返回字段组名称（如果存在）"""
        logger.debug(f"GraphUtils: 查找字段 {field_name}:{field_type} 在模式 {schema_name} 中的共享字段组")
        
        for field_hash, group_info in field_groups.items():
            if group_info['schema'] == schema_name:
                # 检查字段是否在这个字段组中
                column_names = group_info['column_names']
                column_types = group_info['column_types']
                
                logger.debug(f"GraphUtils:   检查字段组 {group_info['group_name']} (哈希: {field_hash[:8]}...)")
                logger.debug(f"GraphUtils:     字段组合: {column_names}")
                logger.debug(f"GraphUtils:     类型组合: {column_types}")
                
                for i, col_name in enumerate(column_names):
                    col_type = column_types[i] if i < len(column_types) else "UNKNOWN"
                    if col_name == field_name and col_type == field_type:
                        logger.debug(f"GraphUtils:     ✓ 匹配到字段组: {group_info['group_name']}")
                        return group_info['group_name']
        
        logger.debug(f"GraphUtils: 未找到包含字段 {field_name}:{field_type} 的共享字段组")
        return None


class FieldGroupOptimizer:
    """字段组优化器 - 使用精确匹配策略"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def optimize_field_groups_with_exact_matching(self, field_groups: Dict[str, List]) -> Dict[str, Dict]:
        """
        优化字段组，确保没有重叠，每个表只属于一个字段组
        使用贪心策略：优先保留共享表最多的字段组
        
        Args:
            field_groups: {field_hash: [(table_info, schema_name, json_file), ...]}
        Returns:
            优化后的字段组信息
        """
        self.logger.info("开始字段组优化（精确匹配模式）...")
        
        # 1. 预处理：计算每个字段组的信息
        group_infos = {}
        for field_hash, tables in field_groups.items():
            if len(tables) < 2:  # 跳过只有一个表的组
                continue
                
            # 获取字段信息
            table_info = tables[0][0]  # 使用第一个表的信息作为参考
            column_names = table_info.get('column_names', [])
            column_types = table_info.get('column_types', [])
            
            # 构建字段集合标识
            field_set = frozenset(f"{name}:{type_}" for name, type_ in zip(column_names, column_types))
            
            group_infos[field_hash] = {
                'tables': tables,
                'table_count': len(tables),
                'field_count': len(column_names),
                'field_set': field_set,
                'column_names': column_names,
                'column_types': column_types
            }
        
        # 2. 按共享表数量和字段数量排序（优先选择共享表多的，字段多的）
        sorted_groups = sorted(
            group_infos.items(),
            key=lambda x: (x[1]['table_count'], x[1]['field_count']),
            reverse=True
        )
        
        # 3. 贪心选择不重叠的字段组
        optimized_groups = {}
        assigned_tables = set()  # 记录已分配的表
        
        for field_hash, group_info in sorted_groups:
            # 检查这个组的表是否已被分配
            current_tables = set(table_info['table_name'] for table_info, _, _ in group_info['tables'])
            if not current_tables & assigned_tables:  # 如果没有重叠的表
                # 添加到优化后的组
                optimized_groups[field_hash] = {
                    'group_name': f"FieldGroup_{field_hash[:8]}",
                    'schema': group_info['tables'][0][1],  # 使用第一个表的schema
                    'field_count': group_info['field_count'],
                    'table_count': group_info['table_count'],
                    'column_names': group_info['column_names'],
                    'column_types': group_info['column_types'],
                    'tables': group_info['tables']
                }
                # 记录已分配的表
                assigned_tables.update(current_tables)
                
                self.logger.info(f"添加字段组: {field_hash[:8]}")
                self.logger.info(f"  - 表数量: {len(current_tables)}")
                self.logger.info(f"  - 字段数量: {group_info['field_count']}")
                self.logger.info(f"  - 表: {', '.join(current_tables)}")
        
        self.logger.info(f"字段组优化完成，保留 {len(optimized_groups)} 个非重叠字段组")
        return optimized_groups
    
    def _analyze_field_combinations(self, field_groups_data: Dict[str, List]) -> List[Dict]:
        """分析字段组合（仅用于日志记录和调试）"""
        combinations = []
        for field_hash, tables in field_groups_data.items():
            if len(tables) < 2:  # 跳过单表组合
                continue
            
            table_info = tables[0][0]  # 使用第一个表的信息
            column_names = table_info.get('column_names', [])
            column_types = table_info.get('column_types', [])
            
            # 构建字段集合
            field_set = set(f"{name}:{type_}" for name, type_ in zip(column_names, column_types))
            
            combinations.append({
                'field_hash': field_hash,
                'table_count': len(tables),
                'field_count': len(column_names),
                'field_set': field_set,
                'tables': [t[0]['table_name'] for t in tables]
            })
        
        return combinations
    
    def optimize_field_groups(self, field_groups_data: Dict[str, List]) -> Dict[str, Dict]:
        """
        优化字段组，确保最小不重叠
        Args:
            field_groups_data: {field_hash: [(table_info, schema_name, json_file), ...]}
        Returns:
            optimized_groups: {field_hash: group_info}
        """
        self.logger.info("开始优化字段组，确保最小不重叠...")
        
        # 第一步：分析所有字段组合
        field_combinations = self._analyze_field_combinations(field_groups_data)
        
        # 第二步：构建包含关系图
        containment_graph = self._build_containment_graph(field_combinations)
        
        # 第三步：选择最优字段组集合
        optimal_groups = self._select_optimal_groups(field_combinations, containment_graph)
        
        # 第四步：验证结果
        self._validate_optimization(optimal_groups, field_groups_data)
        
        return optimal_groups
    
    def _analyze_field_combinations(self, field_groups_data: Dict[str, List]) -> List[Dict]:
        """分析所有字段组合"""
        combinations = []
        
        for field_hash, tables_with_fields in field_groups_data.items():
            if len(tables_with_fields) > 1:  # 只处理多表共享的字段组
                representative_table, representative_schema, _ = tables_with_fields[0]
                column_names = representative_table.get('column_names', [])
                column_types = representative_table.get('column_types', [])
                
                if len(column_names) >= 2:  # 只处理多字段组合
                    # 创建字段集合（字段名:类型）
                    field_set = set()
                    for i, name in enumerate(column_names):
                        col_type = column_types[i] if i < len(column_types) else "UNKNOWN"
                        field_set.add(f"{name}:{col_type}")
                    
                    combination = {
                        'field_hash': field_hash,
                        'field_set': field_set,
                        'field_names': column_names,
                        'field_types': column_types,
                        'schema': representative_schema,
                        'representative_table': representative_table.get('table_name', ''),
                        'table_count': len(tables_with_fields),
                        'field_count': len(column_names),
                        'tables': [info[0].get('table_name', '') for info in tables_with_fields]
                    }
                    combinations.append(combination)
        
        # 按字段数量降序，表数量降序排序
        combinations.sort(key=lambda x: (x['field_count'], x['table_count']), reverse=True)
        
        self.logger.info(f"分析到 {len(combinations)} 个字段组合")
        for combo in combinations:
            self.logger.info(f"  {combo['field_hash'][:8]}... : {combo['field_count']}字段 x {combo['table_count']}表")
        
        return combinations
    
    def _build_containment_graph(self, combinations: List[Dict]) -> Dict[str, Dict]:
        """构建包含关系图"""
        containment_graph = {}
        
        for i, combo_a in enumerate(combinations):
            hash_a = combo_a['field_hash']
            containment_graph[hash_a] = {
                'contains': [],      # 包含的字段组
                'contained_by': [],  # 被包含的字段组
                'overlaps': []       # 重叠的字段组
            }
            
            for j, combo_b in enumerate(combinations):
                if i != j:
                    hash_b = combo_b['field_hash']
                    set_a = combo_a['field_set']
                    set_b = combo_b['field_set']
                    
                    if set_a.issuperset(set_b):
                        # A 包含 B
                        containment_graph[hash_a]['contains'].append(hash_b)
                    elif set_a.issubset(set_b):
                        # A 被 B 包含
                        containment_graph[hash_a]['contained_by'].append(hash_b)
                    elif len(set_a.intersection(set_b)) > 0:
                        # A 和 B 重叠
                        containment_graph[hash_a]['overlaps'].append(hash_b)
        
        # 打印包含关系分析
        self.logger.info("字段组包含关系分析:")
        for hash_key, relations in containment_graph.items():
            combo = next(c for c in combinations if c['field_hash'] == hash_key)
            self.logger.info(f"  {hash_key[:8]}... ({combo['field_count']}字段):")
            if relations['contains']:
                self.logger.info(f"    包含: {len(relations['contains'])} 个字段组")
            if relations['contained_by']:
                self.logger.info(f"    被包含: {len(relations['contained_by'])} 个字段组")
            if relations['overlaps']:
                self.logger.info(f"    重叠: {len(relations['overlaps'])} 个字段组")
        
        return containment_graph
    
    def _select_optimal_groups(self, combinations: List[Dict], containment_graph: Dict[str, Dict]) -> Dict[str, Dict]:
        """选择最优字段组集合（贪心算法）"""
        selected_groups = {}
        selected_hashes = set()
        
        self.logger.info("开始选择最优字段组集合...")
        
        # 贪心算法：优先选择字段多、表多且不与已选择字段组重叠的字段组
        for combo in combinations:
            field_hash = combo['field_hash']
            
            # 检查是否与已选择的字段组重叠
            can_select = True
            conflict_reason = ""
            
            for selected_hash in selected_hashes:
                if field_hash in containment_graph[selected_hash]['contains']:
                    can_select = False
                    conflict_reason = f"被已选择字段组包含 ({selected_hash[:8]}...)"
                    break
                elif selected_hash in containment_graph[field_hash]['contains']:
                    can_select = False
                    conflict_reason = f"包含已选择字段组 ({selected_hash[:8]}...)"
                    break
                elif field_hash in containment_graph[selected_hash]['overlaps']:
                    can_select = False
                    conflict_reason = f"与已选择字段组重叠 ({selected_hash[:8]}...)"
                    break
            
            if can_select:
                selected_groups[field_hash] = {
                    'group_name': self._generate_optimized_group_name(combo),
                    'schema': combo['schema'],
                    'column_names': combo['field_names'],
                    'column_types': combo['field_types'],
                    'table_count': combo['table_count'],
                    'field_count': combo['field_count'],
                    'tables': combo['tables']
                }
                selected_hashes.add(field_hash)
                self.logger.info(f"  ✓ 选择字段组: {field_hash[:8]}... ({combo['field_count']}字段 x {combo['table_count']}表)")
            else:
                self.logger.info(f"  ✗ 跳过字段组: {field_hash[:8]}... ({conflict_reason})")
        
        self.logger.info(f"最终选择了 {len(selected_groups)} 个不重叠字段组")
        return selected_groups
    
    def _generate_optimized_group_name(self, combo: Dict) -> str:
        """生成优化后的字段组名"""
        schema = combo['schema']
        representative_table = combo['representative_table']
        field_count = combo['field_count']
        field_hash = combo['field_hash']
        
        # 简化表名
        base_name = representative_table.replace(f"{schema}.", "")
        
        # 常见模式清理
        patterns = [
            r'_\d{4}_Q\d$',  # _1998_Q1
            r'_\d{4}$',      # _2020
            r'_\d{6}$',      # _202012
            r'_\d{8}$',      # _20201231
            r'_v\d+$',       # _v1, _v2
            r'_\d+$',        # _1, _2, _3
        ]
        
        for pattern in patterns:
            base_name = re.sub(pattern, '', base_name)
        
        # 生成优化后的名称
        return f"{schema}.{base_name}_OptimizedGroup_{field_count}F_{field_hash[:8]}"
    
    def _validate_optimization(self, optimal_groups: Dict[str, Dict], original_data: Dict[str, List]):
        """验证优化结果"""
        self.logger.info("验证优化结果...")
        
        # 检查覆盖率
        original_table_count = sum(len(tables) for tables in original_data.values() if len(tables) > 1)
        optimized_table_count = sum(group['table_count'] for group in optimal_groups.values())
        
        self.logger.info(f"  原始覆盖表数: {original_table_count}")
        self.logger.info(f"  优化后覆盖表数: {optimized_table_count}")
        if original_table_count > 0:
            self.logger.info(f"  覆盖率: {optimized_table_count/original_table_count*100:.1f}%")
        
        # 检查字段组重叠
        all_field_sets = []
        for group in optimal_groups.values():
            field_set = set()
            for i, name in enumerate(group['column_names']):
                col_type = group['column_types'][i] if i < len(group['column_types']) else "UNKNOWN"
                field_set.add(f"{name}:{col_type}")
            all_field_sets.append(field_set)
        
        has_overlap = False
        for i, set_a in enumerate(all_field_sets):
            for j, set_b in enumerate(all_field_sets):
                if i < j and len(set_a.intersection(set_b)) > 0:
                    has_overlap = True
                    self.logger.warning(f"  发现重叠: 字段组{i} 和 字段组{j}")
        
        if not has_overlap:
            self.logger.info("  ✓ 验证通过: 所有字段组都不重叠")
        else:
            self.logger.warning("  ✗ 验证失败: 发现字段组重叠") 