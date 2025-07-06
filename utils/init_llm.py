from langchain.chat_models import init_chat_model
import os
import logging

def initialize_llm() :
    """
    初始化LLM实例
    
    Returns:
        LLM实例或None
    """
    # 读取环境变量
    model = os.getenv("LLM_MODEL") or "gpt-4o-mini"
    
    
    try:
        
        llm = init_chat_model(model=model, model_provider="openai")
        # 测试LLM连接（简化测试）
        try:
            test_result = llm.invoke("Hello")
            if test_result:
                logging.info(f"LLM连接测试成功")
            else:
                logging.warning("LLM连接测试返回空结果")
        except Exception as e:
            logging.warning(f"LLM连接测试失败，但继续运行: {e}")
        
        return llm
        
    except Exception as e:
        logging.error(f"LLM初始化失败: {e}")
        return None