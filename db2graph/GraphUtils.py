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
    """字段组优化器，用于创建最小不重叠字段组集合"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def optimize_field_groups_with_exact_matching(self, field_groups_data: Dict[str, List]) -> Dict[str, Dict]:
        """
        优化字段组并确保精确匹配（最小子集解决方案）
        Args:
            field_groups_data: {field_hash: [(table_info, schema_name, json_file), ...]}
        Returns:
            optimized_groups: {field_hash: group_info}
        """
        self.logger.info("开始优化字段组，使用精确字段集合匹配...")
        
        # 第一步：分析所有字段组合并按模式分组
        schema_combinations = self._analyze_combinations_by_schema(field_groups_data)
        
        # 第二步：为每个模式选择最小覆盖字段组集合
        optimal_groups = {}
        for schema_name, combinations in schema_combinations.items():
            schema_optimal = self._select_minimal_covering_set(schema_name, combinations)
            optimal_groups.update(schema_optimal)
        
        # 第三步：验证精确匹配
        self._validate_exact_matching(optimal_groups, field_groups_data)
        
        return optimal_groups
    
    def _analyze_combinations_by_schema(self, field_groups_data: Dict[str, List]) -> Dict[str, List[Dict]]:
        """按模式分析字段组合"""
        schema_combinations = defaultdict(list)
        
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
                    schema_combinations[representative_schema].append(combination)
        
        # 为每个模式按字段数量排序
        for schema in schema_combinations:
            schema_combinations[schema].sort(key=lambda x: x['field_count'], reverse=True)
        
        return dict(schema_combinations)
    
    def _select_minimal_covering_set(self, schema_name: str, combinations: List[Dict]) -> Dict[str, Dict]:
        """为单个模式选择最小覆盖字段组集合"""
        self.logger.info(f"为模式 {schema_name} 选择最小覆盖字段组集合...")
        
        # 使用贪心算法选择最小覆盖集合
        selected_groups = {}
        covered_tables = set()
        
        # 按表数量和字段数量排序，优先选择覆盖更多表的字段组
        combinations.sort(key=lambda x: (x['table_count'], x['field_count']), reverse=True)
        
        for combo in combinations:
            field_hash = combo['field_hash']
            combo_tables = set(combo['tables'])
            
            # 检查是否与已选择的字段组有冲突
            has_conflict = False
            for selected_hash, selected_info in selected_groups.items():
                selected_set = selected_info['field_set']
                combo_set = combo['field_set']
                
                # 如果字段集合有重叠且不完全相同，则有冲突
                if (len(selected_set.intersection(combo_set)) > 0 and 
                    selected_set != combo_set):
                    has_conflict = True
                    break
            
            if not has_conflict:
                # 计算新增覆盖的表数量
                new_tables = combo_tables - covered_tables
                if len(new_tables) > 0:  # 只选择能覆盖新表的字段组
                    selected_groups[field_hash] = {
                        'field_set': combo['field_set'],
                        'group_name': self._generate_optimized_group_name(combo),
                        'schema': combo['schema'],
                        'column_names': combo['field_names'],
                        'column_types': combo['field_types'],
                        'table_count': combo['table_count'],
                        'field_count': combo['field_count'],
                        'tables': combo['tables']
                    }
                    covered_tables.update(combo_tables)
                    self.logger.info(f"  ✓ 选择字段组: {field_hash[:8]}... ({combo['field_count']}字段 x {combo['table_count']}表)")
                    self.logger.info(f"    新增覆盖表: {len(new_tables)} 个")
                else:
                    self.logger.info(f"  ✗ 跳过字段组: {field_hash[:8]}... (不覆盖新表)")
            else:
                self.logger.info(f"  ✗ 跳过字段组: {field_hash[:8]}... (与已选字段组冲突)")
        
        self.logger.info(f"模式 {schema_name} 选择了 {len(selected_groups)} 个字段组，覆盖 {len(covered_tables)} 个表")
        return selected_groups
    
    def _validate_exact_matching(self, optimal_groups: Dict[str, Dict], original_data: Dict[str, List]):
        """验证精确匹配结果"""
        self.logger.info("验证精确字段集合匹配...")
        
        validation_errors = []
        
        # 为每个字段组验证其包含的表是否都有完全相同的字段集合
        for field_hash, group_info in optimal_groups.items():
            group_field_set = group_info['field_set']
            tables_in_group = set(group_info['tables'])
            
            # 检查原始数据中这个字段组对应的所有表
            original_tables = original_data.get(field_hash, [])
            for table_info, schema_name, _ in original_tables:
                table_name = table_info.get('table_name', '')
                column_names = table_info.get('column_names', [])
                column_types = table_info.get('column_types', [])
                
                # 构建这个表的字段集合
                table_field_set = set()
                for i, name in enumerate(column_names):
                    col_type = column_types[i] if i < len(column_types) else "UNKNOWN"
                    table_field_set.add(f"{name}:{col_type}")
                
                # 验证字段集合是否完全匹配
                if table_field_set != group_field_set:
                    validation_errors.append(
                        f"表 {table_name} 的字段集合与字段组 {group_info['group_name']} 不完全匹配"
                    )
                    self.logger.warning(f"  字段集合不匹配: {table_name}")
                    self.logger.warning(f"    表字段: {sorted(table_field_set)}")
                    self.logger.warning(f"    组字段: {sorted(group_field_set)}")
        
        if validation_errors:
            self.logger.error(f"发现 {len(validation_errors)} 个精确匹配验证错误")
            for error in validation_errors:
                self.logger.error(f"  {error}")
            return False
        else:
            self.logger.info("✓ 精确字段集合匹配验证通过")
            return True
    
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