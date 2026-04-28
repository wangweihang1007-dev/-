import time
import requests
import os
import re
import json
from pathlib import Path
from loguru import logger
import glob
from zhipuai import ZhipuAI

class DataFlowFactory:
    """
    智能 AI 数据工厂类 (封装版)
    """
    def __init__(self, mineru_key, zhipu_key):
        self.mineru_key = mineru_key
        self.zhipu_client = ZhipuAI(api_key=zhipu_key)
        
        # 路径配置
        self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.input_dir = os.path.join(self.base_dir, "data")
        self.output_md = os.path.join(self.base_dir, "output", "markdown")
        self.output_jsonl = os.path.join(self.base_dir, "output", "jsonl", "structured_data.jsonl")
        self.output_alpaca = os.path.join(self.base_dir, "output", "dataset", "alpaca_training_data.json")
        
        # 初始化输出平衡结构
        for d in [self.output_md, os.path.dirname(self.output_jsonl), os.path.dirname(self.output_alpaca)]:
            Path(d).mkdir(parents=True, exist_ok=True)

    def clean_text(self, text):
        """核心去噪算子"""
        if not text: return ""
        patterns = [
            r"(?i)confidential", r"[机機][密⽬][文⽂][件⽗]", r"仅供内部评审使[用⽤]", 
            r"Do Not Distribute", r"第\s*\d+\s*.\s*(?:/|共|之)\s*\d+\s*.", 
            r"Page\s*\d+\s*(?:of|/)\s*\d+", r"-\s*\d+\s*-", r"【.*?】"
        ]
        for p in patterns: text = re.sub(p, "", text)
        lines = [l for l in text.split('\n') if not re.match(r"^[^\w\u4e00-\u9fa5\s\-\*\|]{3,}$", l.strip())]
        return re.sub(r"\n{3,}", "\n\n", '\n'.join(lines)).strip()

    def mask_pii(self, text):
        """身份与隐私脱敏算子"""
        text = re.sub(r"(?<!\d)1[3-9]\d{9}(?!\d)", "[PHONE]", text)
        text = re.sub(r"(?<!\d)\d{17}[\dXx](?!\d)", "[ID_CARD]", text)
        text = re.sub(r"(?m)^(?:作者信息|项目负责人|通讯作者)[:：].*$", '', text)
        return text

    def run_full_process(self):
        """全量流水线自动化执行"""
        logger.info(">>> 启动 DataFlow 工厂流水线 <<<")
        md_files = glob.glob(os.path.join(self.output_md, "*.md"))
        
        all_dataset = []
        for f in md_files:
            fname = os.path.basename(f)
            logger.info(f"正在加工: {fname}")
            with open(f, "r", encoding="utf-8") as raw:
                # 级联清洗与脱敏
                clean_content = self.mask_pii(self.clean_text(raw.read()))
                
                # 语义分段
                sections = []
                for p in re.split(r'(^#+\s+.*$)', clean_content, flags=re.MULTILINE):
                    if p.strip(): sections.append({"text": p.strip()})
                
                # 导出 JSONL
                with open(self.output_jsonl, "a", encoding="utf-8") as jf:
                    jf.write(json.dumps({"source": fname, "content": clean_content}, ensure_ascii=False) + "\n")
                
                # 调用智能体生成 Alpaca (此处为示例频率，可根据需求开启)
                # dataset = self.transform_logic_here(sections) 
                
        logger.success(">>> 流水线全量执行任务结束 <<<")
