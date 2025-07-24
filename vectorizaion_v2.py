# 这个文件专门负责向量化处理

import os
import json
import logging
import numpy as np
from typing import List, Dict, Tuple, Optional
from pathlib import Path
from dotenv import load_dotenv
import faiss
from openai import OpenAI
from tqdm import tqdm
from utils.CypherExecutor import CypherExecutor

## 查询所有的数据库
GET_ALL_DATABASES = """
MATCH (db:Database)
RETURN db.name AS database_name
ORDER BY db.name
"""

class Vectorization:
    """
    向量化处理器
    专门负责将已有描述的数据库节点进行向量化处理
    
    处理的节点类型：
    1. standalone表（不连接SharedFieldGroup的Table）
    2. SharedFieldGroup节点
    
    向量化数据存储在resource/vector下面，按照数据库名称组织：
    - faiss_index_<database_name>.bin: FAISS索引文件
    - metadata_<database_name>.jsonl: 元数据文件
    """
    
    def __init__(self, enable_info_logging=False):
        """
        初始化向量化处理器
        
        Args:
            enable_info_logging (bool): 是否启用info级别日志，默认为False
        """
        # 创建独立的logger
        self.logger = logging.getLogger(f"{__name__}.Vectorization")
        self.setup_logging()
        
        # 加载环境变量
        load_dotenv(".env")
        
        # 初始化OpenAI客户端
        self.openai_client = OpenAI(
            api_key=os.getenv("OPENAI_API_KEY"),
            base_url=os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1")
        )
        
        # 初始化数据库连接
        self.cypher_executor = CypherExecutor(enable_info_logging=False)
        
        # 设置向量目录
        self.vector_dir = Path("resource/vector")
        self.vector_dir.mkdir(parents=True, exist_ok=True)
        
        # 向量维度
        self.embedding_dim = 1536  # text-embedding-3-small的维度
        
    def setup_logging(self):
        """设置独立的日志配置"""
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            
        self.logger.setLevel(logging.ERROR)
        self.logger.propagate = False
    
    def get_all_databases(self) -> List[str]:
        """
        获取所有数据库名称
        
        Returns:
            List[str]: 数据库名称列表
        """
        success, results = self.cypher_executor.execute_transactional_cypher(GET_ALL_DATABASES)
        
        if not success:
            self.logger.error("获取所有数据库失败")
            return []
        
        databases = [result['database_name'] for result in results]
        self.logger.info(f"找到 {len(databases)} 个数据库: {databases}")
        return databases
    
    def get_vectorizable_items(self, database_name: str) -> Tuple[List[Dict], List[Dict]]:
        """
        获取数据库下所有有描述的项目进行向量化
        注意：每次都重新生成向量，不检查是否已经向量化过
        
        Args:
            database_name (str): 数据库名称
            
        Returns:
            Tuple[List[Dict], List[Dict]]: (standalone表列表, SharedFieldGroup列表)
        """
        # 获取所有有描述的standalone表
        standalone_query = """
        MATCH (db:Database {name: $database_name})-[:HAS_SCHEMA]->(:Schema)-[:HAS_TABLE]->(t:Table)
        WHERE NOT (t)-[:USES_FIELD_GROUP]->(:SharedFieldGroup)
          AND t.description IS NOT NULL
          AND t.description <> ''
        RETURN t.name AS table_name, t.schema AS schema_name, t.database AS database_name, 
               t.description AS description, elementId(t) AS element_id
        ORDER BY t.name
        """
        
        # 获取所有有描述的SharedFieldGroup
        shared_group_query = """
        MATCH (db:Database {name: $database_name})-[:HAS_SCHEMA]->(:Schema)-[:HAS_TABLE]->(:Table)-[:USES_FIELD_GROUP]->(sfg:SharedFieldGroup)
        WHERE sfg.description IS NOT NULL
          AND sfg.description <> ''
        RETURN DISTINCT sfg.name AS group_name, sfg.description AS description, elementId(sfg) AS element_id
        ORDER BY sfg.name
        """
        
        parameters = {"database_name": database_name}
        
        # 获取standalone表
        success1, tables = self.cypher_executor.execute_transactional_cypher(standalone_query, parameters)
        if not success1:
            self.logger.error(f"获取数据库 {database_name} standalone表失败")
            tables = []
        
        # 获取SharedFieldGroup
        success2, groups = self.cypher_executor.execute_transactional_cypher(shared_group_query, parameters)
        if not success2:
            self.logger.error(f"获取数据库 {database_name} SharedFieldGroup失败")
            groups = []
        
        self.logger.info(f"数据库 {database_name} 可向量化项目: standalone表 {len(tables)} 个, SharedFieldGroup {len(groups)} 个")
        
        return tables, groups
    
    def format_item_for_vectorization(self, item: Dict, item_type: str) -> str:
        """
        将项目格式化为向量化文本
        
        Args:
            item (Dict): 项目信息
            item_type (str): 项目类型 ('table' 或 'shared_group')
            
        Returns:
            str: 格式化的向量化文本
        """
        if item_type == 'table':
            table_name = item.get('table_name', 'unknown')
            schema_name = item.get('schema_name', '')
            database_name = item.get('database_name', 'unknown')
            description = item.get('description', '').strip()
            
            # 构建完整的表名
            if schema_name:
                full_table_name = f"{schema_name}.{table_name}"
            else:
                full_table_name = table_name
            
            return f"Table {full_table_name} in database {database_name}. Description: {description}"
            
        elif item_type == 'shared_group':
            group_name = item.get('group_name', 'unknown')
            description = item.get('description', '').strip()
            
            return f"SharedFieldGroup {group_name}. Description: {description}"
        
        else:
            return f"Unknown item type: {item_type}"
    
    def get_embeddings(self, texts: List[str]) -> List[List[float]]:
        """
        使用OpenAI API获取文本向量
        
        Args:
            texts (List[str]): 待向量化的文本列表
            
        Returns:
            List[List[float]]: 向量列表
        """
        try:
            response = self.openai_client.embeddings.create(
                model="text-embedding-3-small",
                input=texts
            )
            
            embeddings = [data.embedding for data in response.data]
            self.logger.info(f"生成了 {len(texts)} 个文本的向量")
            return embeddings
            
        except Exception as e:
            self.logger.error(f"生成向量失败: {e}")
            return []
    
    def build_faiss_index(self, embeddings: List[List[float]]) -> faiss.IndexFlatIP:
        """
        构建FAISS索引
        
        Args:
            embeddings (List[List[float]]): 向量列表
            
        Returns:
            faiss.IndexFlatIP: FAISS索引
        """
        embeddings_array = np.array(embeddings, dtype=np.float32)
        
        # 归一化向量以使用内积进行余弦相似度计算
        faiss.normalize_L2(embeddings_array)
        
        # 创建索引
        index = faiss.IndexFlatIP(self.embedding_dim)
        index.add(embeddings_array)
        
        self.logger.info(f"构建FAISS索引，包含 {index.ntotal} 个向量")
        return index
    
    def vectorize_database(self, database_name: str, embedding_batch_size: int = 50, 
                          show_progress: bool = True) -> bool:
        """
        为指定数据库进行向量化处理
        
        Args:
            database_name (str): 数据库名称
            embedding_batch_size (int): 向量化批次大小
            show_progress (bool): 是否显示进度
            
        Returns:
            bool: 是否成功
        """
        self.logger.info(f"开始为数据库 {database_name} 进行向量化")
        
        try:
            # 获取需要向量化的项目
            tables, groups = self.get_vectorizable_items(database_name)
            self.logger.info(f"获取到可向量化项目: {len(tables)} 个表, {len(groups)} 个SharedFieldGroup")
            
            # 合并所有项目
            all_items = []
            all_texts = []
            
            # 添加表项目
            for table in tables:
                self.logger.info(f"准备向量化表: {table['table_name']}")
                item_info = {
                    "type": "table",
                    "name": table['table_name'],
                    "schema": table.get('schema_name', ''),
                    "database": table['database_name'],
                    "element_id": table['element_id'],
                    "description": table['description']
                }
                all_items.append(item_info)
                all_texts.append(self.format_item_for_vectorization(table, 'table'))
            
            # 添加SharedFieldGroup项目
            for group in groups:
                self.logger.info(f"准备向量化SharedFieldGroup: {group['group_name']}")
                item_info = {
                    "type": "shared_group",
                    "name": group['group_name'],
                    "element_id": group['element_id'],
                    "description": group['description']
                }
                all_items.append(item_info)
                all_texts.append(self.format_item_for_vectorization(group, 'shared_group'))
            
            if not all_items:
                self.logger.info(f"数据库 {database_name} 没有可向量化的项目")
                return True
            
            self.logger.info(f"数据库 {database_name} 共有 {len(all_items)} 个项目需要向量化")
            
            # 分批处理向量化
            all_embeddings = []
            total_batches = (len(all_texts) + embedding_batch_size - 1) // embedding_batch_size
            
            batch_range = range(0, len(all_texts), embedding_batch_size)
            if show_progress:
                batch_range = tqdm(batch_range, desc=f"向量化 {database_name}", unit="批次", total=total_batches)
            
            for i in batch_range:
                batch_texts = all_texts[i:i + embedding_batch_size]
                batch_embeddings = self.get_embeddings(batch_texts)
                
                if not batch_embeddings:
                    batch_num = i // embedding_batch_size + 1
                    self.logger.error(f"批次 {batch_num} 向量化失败")
                    return False
                
                all_embeddings.extend(batch_embeddings)
                
                if show_progress:
                    batch_range.set_postfix({
                        '已处理': len(all_embeddings),
                        '总计': len(all_texts)
                    })
            
            # 构建FAISS索引
            index = self.build_faiss_index(all_embeddings)
            
            # 保存索引文件
            index_path = self.vector_dir / f"faiss_index_{database_name}.bin"
            faiss.write_index(index, str(index_path))
            
            # 保存元数据
            metadata_path = self.vector_dir / f"metadata_{database_name}.jsonl"
            with open(metadata_path, 'w', encoding='utf-8') as f:
                for i, item in enumerate(all_items):
                    metadata = {
                        'vector_index': i,
                        'item_type': item['type'],
                        'name': item['name'],
                        'element_id': item['element_id'],
                        'description': item['description']
                    }
                    if item['type'] == 'table':
                        metadata['schema'] = item['schema']
                        metadata['database'] = item['database']
                    
                    f.write(json.dumps(metadata, ensure_ascii=False) + '\n')
            
            self.logger.info(f"数据库 {database_name} 向量化完成，保存到:")
            self.logger.info(f"  索引文件: {index_path}")
            self.logger.info(f"  元数据文件: {metadata_path}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"数据库 {database_name} 向量化失败: {e}")
            return False
    
    def load_database_index(self, database_name: str) -> Tuple[Optional[faiss.IndexFlatIP], Optional[List[Dict]]]:
        """
        加载指定数据库的向量索引和元数据
        
        Args:
            database_name (str): 数据库名称
            
        Returns:
            Tuple[Optional[faiss.IndexFlatIP], Optional[List[Dict]]]: 索引和元数据
        """
        index_path = self.vector_dir / f"faiss_index_{database_name}.bin"
        metadata_path = self.vector_dir / f"metadata_{database_name}.jsonl"
        
        if not index_path.exists() or not metadata_path.exists():
            self.logger.error(f"数据库 {database_name} 的向量文件不存在")
            return None, None
        
        try:
            # 加载索引
            index = faiss.read_index(str(index_path))
            
            # 加载元数据
            metadata = []
            with open(metadata_path, 'r', encoding='utf-8') as f:
                for line in f:
                    metadata.append(json.loads(line.strip()))
            
            self.logger.info(f"加载数据库 {database_name} 索引: {index.ntotal} 个向量")
            return index, metadata
            
        except Exception as e:
            self.logger.error(f"加载数据库 {database_name} 索引失败: {e}")
            return None, None
    
    def search_items(self, query: str, database_name: str, top_k: int = 5) -> List[Dict]:
        """
        在指定数据库中搜索相关项目
        
        Args:
            query (str): 查询文本
            database_name (str): 数据库名称
            top_k (int): 返回结果数量
            
        Returns:
            List[Dict]: 搜索结果
        """
        # 加载索引
        index, metadata = self.load_database_index(database_name)
        if index is None or metadata is None:
            return []
        
        # 向量化查询
        query_embeddings = self.get_embeddings([query])
        if not query_embeddings:
            self.logger.error("查询向量化失败")
            return []
        
        query_vector = np.array([query_embeddings[0]], dtype=np.float32)
        faiss.normalize_L2(query_vector)
        
        # 搜索
        scores, indices = index.search(query_vector, min(top_k, len(metadata)))
        
        # 格式化结果
        results = []
        for i, (score, idx) in enumerate(zip(scores[0], indices[0])):
            if idx != -1:  # 有效索引
                result = metadata[idx].copy()
                result['similarity_score'] = float(score)
                result['rank'] = i + 1
                results.append(result)
        
        self.logger.info(f"在数据库 {database_name} 中找到 {len(results)} 个相关结果")
        return results
    
    def close(self):
        """关闭连接"""
        if self.cypher_executor:
            self.cypher_executor.close()


def main():
    """
    主函数 - 对所有数据库进行向量化处理
    注意: 确保在运行此程序前已经完成了描述生成 (运行 gen_des.py)
    """
    # 设置日志 - 只显示ERROR级别
    logging.basicConfig(
        level=logging.ERROR,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    print("=== 数据库向量化系统 ===")
    
    # 初始化向量化器
    vectorizer = Vectorization()
    
    try:
        # 验证数据库连接
        if not vectorizer.cypher_executor.verify_connectivity():
            print("❌ 数据库连接失败")
            return
        
        # 获取所有数据库
        databases = vectorizer.get_all_databases()
        
        if not databases:
            print("❌ 未找到任何数据库")
            return
        
        # 处理每个数据库
        success_count = 0
        failed_databases = []
        
        # 使用总体进度条
        with tqdm(databases, desc="向量化进度", unit="个") as pbar:
            for database_name in pbar:
                pbar.set_postfix({'当前数据库': database_name})
                
                try:
                    # 检查是否有可向量化的项目
                    tables, groups = vectorizer.get_vectorizable_items(database_name)
                    total_items = len(tables) + len(groups)
                    
                    if total_items == 0:
                        failed_databases.append(f"{database_name}(无可向量化项目)")
                        pbar.set_postfix({'状态': '无可向量化项目'})
                        continue
                    
                    # 向量化处理
                    pbar.set_postfix({'状态': f'向量化中 ({total_items}项)'})
                    vector_success = vectorizer.vectorize_database(
                        database_name, embedding_batch_size=50, show_progress=False
                    )
                    
                    if vector_success:
                        success_count += 1
                        pbar.set_postfix({'状态': '完成'})
                    else:
                        failed_databases.append(f"{database_name}(向量化失败)")
                        pbar.set_postfix({'状态': '失败'})
                
                except Exception as e:
                    logger.error(f"处理数据库 {database_name} 时出现异常: {e}")
                    failed_databases.append(f"{database_name}(异常)")
                    pbar.set_postfix({'状态': '异常'})
                
                # 更新总体进度信息
                pbar.set_postfix({
                    '成功': success_count,
                    '失败': len(failed_databases),
                    '总计': len(databases),
                    '当前': database_name
                })
        
        # 输出最终统计
        print("\n=== 向量化完成统计 ===")
        print(f"总数据库: {len(databases)} | 成功: {success_count} | 失败: {len(failed_databases)}")
        
        if failed_databases:
            print("\n失败的数据库:")
            for failed_db in failed_databases:
                print(f"- {failed_db}")
        
        success_rate = success_count / len(databases) if databases else 0
        print(f"\n成功率: {success_rate:.1%}")
        
        if success_rate < 0.8:
            print("⚠️  成功率较低，请检查失败原因")
        
    except Exception as e:
        logger.error(f"主程序异常: {e}")
        print(f"❌ 程序执行出错")
    
    finally:
        # 关闭连接
        vectorizer.close()


if __name__ == "__main__":
    main()

