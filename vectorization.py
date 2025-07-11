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


class VectorizedFieldManager:
    def __init__(self, enable_info_logging=True):
        """
        初始化向量化字段管理器
        
        Args:
            enable_info_logging (bool): 是否启用info级别日志
        """
        self.enable_info_logging = enable_info_logging
        self.setup_logging()
        
        # 加载环境变量
        load_dotenv(".env")
        
        # 初始化OpenAI客户端
        self.openai_client = OpenAI(
            api_key=os.getenv("OPENAI_API_KEY"),
            base_url=os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1")
        )
        
        # 初始化数据库连接
        self.cypher_executor = CypherExecutor(enable_info_logging=enable_info_logging)
        
        # 设置向量目录
        self.vector_dir = Path("resource/vector")
        self.vector_dir.mkdir(parents=True, exist_ok=True)
        
        # 向量维度
        self.embedding_dim = 1536  # text-embedding-3-small的维度
        
    def setup_logging(self):
        """设置日志配置"""
        # 设置全局日志级别为ERROR，减少噪音
        logging.basicConfig(
            level=logging.ERROR,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
        # 如果启用详细日志，则设置为DEBUG级别
        if self.enable_info_logging:
            logging.getLogger().setLevel(logging.DEBUG)
    
    def _log_info(self, message: str):
        """条件性记录debug日志"""
        if self.enable_info_logging:
            logging.debug(message)
    
    def get_database_list(self) -> List[str]:
        """
        获取所有数据库列表
        
        Returns:
            List[str]: 数据库名称列表
        """
        cypher_query = """
        MATCH (f:Field)
        RETURN DISTINCT f.database as database
        ORDER BY f.database
        """
        
        success, results = self.cypher_executor.execute_transactional_cypher(cypher_query)
        
        if not success:
            logging.error("Failed to fetch database list")
            return []
        
        databases = [result.get('database', 'UNKNOWN') for result in results if result.get('database')]
        self._log_info(f"Found {len(databases)} databases: {databases}")
        return databases
    
    def get_database_field_count(self, database: str) -> int:
        """
        获取指定数据库的字段总数
        
        Args:
            database (str): 数据库名称
            
        Returns:
            int: 字段总数
        """
        cypher_query = """
        MATCH (f:Field)
        WHERE f.database = $database
        RETURN count(f) as total_count
        """
        
        parameters = {"database": database}
        self._log_info(f"Getting field count for database {database}")
        success, results = self.cypher_executor.execute_transactional_cypher(cypher_query, parameters)
        
        if not success or not results:
            logging.error(f"Failed to get field count for database {database}")
            return 0
        
        count = results[0].get('total_count', 0)
        self._log_info(f"Database {database} has {count} fields")
        return count
    
    def get_database_fields_paginated(self, database: str, page_size: int = 100, 
                                    offset: int = 0) -> List[Dict]:
        """
        分页获取指定数据库的字段信息
        
        Args:
            database (str): 数据库名称
            page_size (int): 每页大小，默认100
            offset (int): 偏移量，默认0
            
        Returns:
            List[Dict]: 字段信息列表
        """
        cypher_query = """
        MATCH (f:Field)
        WHERE f.database = $database
        RETURN elementId(f) as field_id, 
               f.name as field_name, 
               f.type as field_type, 
               f.database as database, 
               f.table as table_name, 
               f.description as description,
               f.schema as schema
        ORDER BY f.table, f.name
        SKIP $offset
        LIMIT $page_size
        """
        
        parameters = {
            "database": database,
            "page_size": page_size,
            "offset": offset
        }
        
        self._log_info(f"Querying fields for database {database}, offset: {offset}, page_size: {page_size}")
        success, results = self.cypher_executor.execute_transactional_cypher(cypher_query, parameters)
        
        if not success:
            logging.error(f"Failed to fetch fields for database {database} (offset: {offset})")
            return []
        
        fields = []
        for result in results:
            field_info = {
                'id': result.get('field_id'),
                'name': result.get('field_name'),
                'type': result.get('field_type'),
                'database': result.get('database'),
                'table': result.get('table_name'),
                'description': result.get('description', ''),
                'schema': result.get('schema', '')
            }
            fields.append(field_info)
        
        return fields
    
    def get_all_database_fields(self, database: str, page_size: int = 100, 
                              show_progress: bool = True) -> List[Dict]:
        """
        获取指定数据库的所有字段，分页加载并显示进度
        
        Args:
            database (str): 数据库名称
            page_size (int): 每页大小，默认100
            show_progress (bool): 是否显示进度，默认True
            
        Returns:
            List[Dict]: 字段信息列表
        """
        # 获取总字段数
        total_count = self.get_database_field_count(database)
        if total_count == 0:
            self._log_info(f"Database {database} has no fields")
            return []
        
        all_fields = []
        offset = 0
        
        # 计算总页数
        total_pages = (total_count + page_size - 1) // page_size
        
        # 使用tqdm显示分页进度
        page_iter = range(total_pages)
        if show_progress:
            page_iter = tqdm(page_iter, desc=f"加载 {database} 字段", unit="页")
        
        for page_num in page_iter:
            # 获取当前页的字段
            fields = self.get_database_fields_paginated(database, page_size, offset)
            
            if not fields:
                logging.warning(f"No fields returned for database {database} at offset {offset}")
                break
            
            all_fields.extend(fields)
            offset += page_size
            
            # 更新进度条描述
            if show_progress:
                page_iter.set_postfix({
                    '已加载': len(all_fields),
                    '总计': total_count
                })
        
        self._log_info(f"Successfully loaded {len(all_fields)} fields for database {database}")
        return all_fields
    
    def get_all_field_nodes(self, target_databases: List[str] = None, 
                          page_size: int = 100, show_progress: bool = True) -> Dict[str, List[Dict]]:
        """
        分页获取Field节点，按database分类，支持进度显示
        
        Args:
            target_databases (List[str], optional): 目标数据库列表，如果为None则获取所有数据库
            page_size (int): 每页大小，默认100
            show_progress (bool): 是否显示进度，默认True
            
        Returns:
            Dict[str, List[Dict]]: 按数据库分类的字段信息
        """
        # 获取数据库列表
        if target_databases is None:
            databases = self.get_database_list()
        else:
            databases = target_databases
        
        if not databases:
            logging.error("No databases found")
            return {}
        
        if show_progress:
            print(f"开始获取 {len(databases)} 个数据库的字段信息...")
        
        fields_by_database = {}
        
        for i, database in enumerate(databases, 1):
            if show_progress:
                print(f"\n[{i}/{len(databases)}] 处理数据库: {database}")
            
            try:
                fields = self.get_all_database_fields(database, page_size, show_progress)
                fields_by_database[database] = fields
                
                if show_progress:
                    print(f"  ✅ 完成数据库 {database}: {len(fields)} 个字段")
                    
            except Exception as e:
                logging.error(f"Failed to load fields for database {database}: {e}")
                fields_by_database[database] = []
                if show_progress:
                    print(f"  ❌ 数据库 {database} 加载失败: {e}")
        
        total_fields = sum(len(fields) for fields in fields_by_database.values())
        self._log_info(f"Retrieved {total_fields} fields from {len(fields_by_database)} databases")
        
        if show_progress:
            print(f"\n=== 获取完成 ===")
            print(f"总计: {total_fields} 个字段，来自 {len(fields_by_database)} 个数据库")
        
        return fields_by_database
    
    def format_field_for_vectorization(self, field_info: Dict) -> str:
        """
        将字段信息格式化为向量化文本
        
        Args:
            field_info (Dict): 字段信息
            
        Returns:
            str: 格式化的向量化文本
        """
        field_name = field_info.get('name', 'unknown')
        field_type = field_info.get('type', 'unknown')
        table_name = field_info.get('table', 'unknown')
        database = field_info.get('database', 'unknown')
        schema = field_info.get('schema', '')
        description = field_info.get('description', '').strip()
        
        # 构建完整的表名（包含schema）
        if schema and schema.strip():
            full_table_name = f"{schema}.{table_name}"
        else:
            full_table_name = table_name
        
        if description:
            desc_text = f"Description: {description}"
        else:
            desc_text = "No description available."
        
        return f"Field {field_name} (type: {field_type}), from table {full_table_name} in database {database}. {desc_text}"
    
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
            self._log_info(f"Generated embeddings for {len(texts)} texts")
            return embeddings
            
        except Exception as e:
            logging.error(f"Failed to generate embeddings: {e}")
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
        
        self._log_info(f"Built FAISS index with {index.ntotal} vectors")
        return index
    
    def save_database_vectors(self, database: str, page_size: int = 100, 
                            embedding_batch_size: int = 50, show_progress: bool = True) -> bool:
        """
        为单个数据库保存向量索引和元数据，支持分页和批量处理
        
        Args:
            database (str): 数据库名称
            page_size (int): 字段分页大小，默认100
            embedding_batch_size (int): 向量化批次大小，默认50
            show_progress (bool): 是否显示进度，默认True
            
        Returns:
            bool: 是否成功保存
        """
        # 分页获取字段
        fields = self.get_all_database_fields(database, page_size, show_progress)
        
        if not fields:
            self._log_info(f"No fields found for database {database}")
            return True
        
        self._log_info(f"Processing {len(fields)} fields for database {database}")
        
        # 分批处理向量化以控制内存使用
        all_embeddings = []
        all_texts = []
        
        # 计算总批次数
        total_batches = (len(fields) + embedding_batch_size - 1) // embedding_batch_size
        
        # 使用tqdm显示向量化进度
        batch_range = range(0, len(fields), embedding_batch_size)
        if show_progress:
            batch_range = tqdm(batch_range, desc=f"向量化 {database}", unit="批次", total=total_batches)
        
        for i in batch_range:
            batch_fields = fields[i:i + embedding_batch_size]
            batch_texts = [self.format_field_for_vectorization(field) for field in batch_fields]
            
            # 获取当前批次的向量
            batch_embeddings = self.get_embeddings(batch_texts)
            if not batch_embeddings:
                batch_num = i // embedding_batch_size + 1
                logging.error(f"Failed to generate embeddings for batch {batch_num} of database {database}")
                return False
            
            all_embeddings.extend(batch_embeddings)
            all_texts.extend(batch_texts)
            
            # 更新进度条描述
            if show_progress:
                batch_range.set_postfix({
                    '已处理': len(all_embeddings),
                    '总计': len(fields)
                })
        
        # 构建索引
        index = self.build_faiss_index(all_embeddings)
        
        # 保存索引文件
        index_path = self.vector_dir / f"faiss_index_{database}.bin"
        faiss.write_index(index, str(index_path))
        
        # 保存元数据
        metadata_path = self.vector_dir / f"metadata_{database}.jsonl"
        with open(metadata_path, 'w', encoding='utf-8') as f:
            for i, field in enumerate(fields):
                metadata = {
                    'vector_index': i,
                    'field_id': field['id'],
                    'field_name': field['name'],
                    'field_type': field['type'],
                    'table': field['table'],
                    'database': field['database'],
                    'schema': field.get('schema', ''),
                    'description': field['description'],
                    'vectorization_text': all_texts[i]
                }
                f.write(json.dumps(metadata, ensure_ascii=False) + '\n')
        
        self._log_info(f"Successfully saved vectors for database {database}")
        return True
    
    def vectorize_database(self, database_name: str, page_size: int = 100, 
                         embedding_batch_size: int = 50, show_progress: bool = True) -> bool:
        """
        为指定数据库生成向量索引
        
        Args:
            database_name (str): 数据库名称
            page_size (int): 字段分页大小，默认100
            embedding_batch_size (int): 向量化批次大小，默认50
            show_progress (bool): 是否显示进度，默认True
            
        Returns:
            bool: 是否成功
        """
        self._log_info(f"Starting vectorization for database: {database_name}")
        
        # 检查数据库是否存在
        databases = self.get_database_list()
        if database_name not in databases:
            logging.error(f"Database {database_name} not found. Available databases: {databases}")
            return False
        
        return self.save_database_vectors(database_name, page_size, embedding_batch_size, show_progress)
    
    def vectorize_all_databases(self, page_size: int = 100, embedding_batch_size: int = 50, 
                              show_progress: bool = True) -> bool:
        """
        为所有数据库生成向量索引
        
        Args:
            page_size (int): 字段分页大小，默认100
            embedding_batch_size (int): 向量化批次大小，默认50
            show_progress (bool): 是否显示进度，默认True
        
        Returns:
            bool: 是否全部成功
        """
        self._log_info("Starting vectorization for all databases")
        
        # 获取数据库列表
        databases = self.get_database_list()
        
        if not databases:
            logging.error("No databases found")
            return False
        
        success_count = 0
        total_count = len(databases)
        
        # 使用tqdm显示数据库处理进度
        db_iter = databases
        if show_progress:
            db_iter = tqdm(databases, desc="全量向量化", unit="数据库")
        
        for database in db_iter:
            try:
                if self.save_database_vectors(database, page_size, embedding_batch_size, show_progress):
                    success_count += 1
                    if show_progress:
                        db_iter.set_postfix({
                            '成功': success_count,
                            '当前': database[:15] + '...' if len(database) > 15 else database
                        })
                else:
                    logging.error(f"Failed to vectorize database: {database}")
            except Exception as e:
                logging.error(f"Exception during vectorization of database {database}: {e}")
        
        if show_progress:
            print(f"\n全量向量化完成: {success_count}/{total_count} 个数据库成功")
        
        self._log_info(f"Vectorization completed: {success_count}/{total_count} databases successful")
        return success_count == total_count
    
    def load_database_index(self, database: str) -> Tuple[Optional[faiss.IndexFlatIP], Optional[List[Dict]]]:
        """
        加载指定数据库的向量索引和元数据
        
        Args:
            database (str): 数据库名称
            
        Returns:
            Tuple[Optional[faiss.IndexFlatIP], Optional[List[Dict]]]: 索引和元数据
        """
        index_path = self.vector_dir / f"faiss_index_{database}.bin"
        metadata_path = self.vector_dir / f"metadata_{database}.jsonl"
        
        if not index_path.exists() or not metadata_path.exists():
            logging.error(f"Index or metadata file not found for database {database}")
            return None, None
        
        try:
            # 加载索引
            index = faiss.read_index(str(index_path))
            
            # 加载元数据
            metadata = []
            with open(metadata_path, 'r', encoding='utf-8') as f:
                for line in f:
                    metadata.append(json.loads(line.strip()))
            
            self._log_info(f"Loaded index for database {database}: {index.ntotal} vectors")
            return index, metadata
            
        except Exception as e:
            logging.error(f"Failed to load index for database {database}: {e}")
            return None, None
    
    def search_fields(self, query: str, database: str, top_k: int = 5) -> List[Dict]:
        """
        在指定数据库中搜索相关字段
        
        Args:
            query (str): 查询文本
            database (str): 数据库名称
            top_k (int): 返回结果数量
            
        Returns:
            List[Dict]: 搜索结果
        """
        # 加载索引
        index, metadata = self.load_database_index(database)
        if index is None or metadata is None:
            return []
        
        # 向量化查询
        query_embeddings = self.get_embeddings([query])
        if not query_embeddings:
            logging.error("Failed to generate query embedding")
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
        
        self._log_info(f"Found {len(results)} results for query in database {database}")
        return results
    
    def close(self):
        """关闭连接"""
        if self.cypher_executor:
            self.cypher_executor.close()


def test():
    """测试和演示函数"""
    print("=== 字段向量化系统测试 ===\n")
    
    # 初始化管理器
    manager = VectorizedFieldManager(enable_info_logging=True)
    
    try:
        # 验证数据库连接
        print("1. 验证数据库连接...")
        if not manager.cypher_executor.verify_connectivity():
            print("❌ 数据库连接失败")
            return
        print("✅ 数据库连接成功\n")
        
        # 获取数据库列表
        print("2. 获取数据库列表...")
        databases = manager.get_database_list()
        
        if not databases:
            print("❌ 没有找到任何数据库")
            return
        
        print(f"✅ 找到 {len(databases)} 个数据库: {databases}")
        
        # 选择一个数据库进行测试（优先选择CRYPTO，如果没有则选择第一个）
        test_database = "CRYPTO" if "CRYPTO" in databases else databases[0]
        
        print(f"\n3. 测试字段获取和向量化格式（使用数据库: {test_database}）...")
        # 获取少量字段用于格式测试
        test_fields = manager.get_database_fields_paginated(test_database, page_size=3)
        
        if test_fields:
            for field in test_fields:
                formatted_text = manager.format_field_for_vectorization(field)
                print(f"   字段ID: {field['id']}")
                print(f"   向量化文本: {formatted_text}")
                print()
        else:
            print(f"   数据库 {test_database} 中没有找到字段")
        
        # 测试单数据库向量化
        print(f"4. 测试单数据库向量化（{test_database}）...")
        if manager.vectorize_database(test_database, page_size=50, embedding_batch_size=10):
            print(f"✅ 数据库 {test_database} 向量化成功")
        else:
            print(f"❌ 数据库 {test_database} 向量化失败")
            return
        print()
        
        # 测试检索功能
        print("5. 测试检索功能...")
        test_queries = [
            "user information",
            "timestamp field", 
            "transaction data",
            "block data",
            "address"
        ]
        
        for query in test_queries:
            print(f"\n查询: '{query}'")
            results = manager.search_fields(query, test_database, top_k=3)
            
            if results:
                print(f"找到 {len(results)} 个相关字段:")
                for result in results:
                    print(f"  {result['rank']}. {result['field_name']} ({result['field_type']})")
                    print(f"     表: {result['table']}")
                    print(f"     相似度: {result['similarity_score']:.3f}")
            else:
                print("  没有找到相关字段")
        
        print("\n=== 测试完成 ===")
        
    except Exception as e:
        logging.error(f"测试过程中出现错误: {e}")
        print(f"❌ 测试失败: {e}")
    
    finally:
        # 关闭连接
        manager.close()


if __name__ == "__main__":
    # test()  # 取消注释以运行测试
    
    # 初始化管理器 - 关闭详细日志以减少输出
    manager = VectorizedFieldManager(enable_info_logging=False)
    
    try:
        print("开始全量向量化...")
        print("注意: 这可能需要较长时间，请耐心等待")
        print("=" * 50)
        
        if manager.vectorize_all_databases(page_size=200, embedding_batch_size=50):
            print("\n🎉 全量向量化成功完成!")
        else:
            print("\n❌ 全量向量化失败")
            
    except KeyboardInterrupt:
        print("\n⚠️  用户中断操作")
    except Exception as e:
        print(f"\n❌ 全量向量化失败: {e}")
        logging.error(f"Vectorization failed with exception: {e}")
    finally:
        manager.close()
        print("连接已关闭")
