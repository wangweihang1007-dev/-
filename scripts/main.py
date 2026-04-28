import os
from dotenv import load_dotenv
from factory import DataFlowFactory
from loguru import logger

def main():
    # 1. 加载根目录下的 .env 文件
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))
    
    # 2. 从环境变量中读取密钥
    mineru_key = os.getenv("MINERU_API_KEY")
    zhipu_key = os.getenv("ZHIPU_API_KEY")
    
    if not mineru_key or not zhipu_key:
        logger.error("API Key 未配置！请检查项目根目录下的 .env 文件。")
        return

    # 3. 实例化工厂类并启动
    factory = DataFlowFactory(mineru_key, zhipu_key)
    factory.run_full_process()

if __name__ == "__main__":
    main()
