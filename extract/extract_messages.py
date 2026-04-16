import json
import asyncio
import os
from typing import List, Optional
from utils.call_llm import call_llm
from utils.read_prompt import read_file
from models.input_data import SMS, SMSList

async def process_sms(raw_text: str, sem: asyncio.Semaphore) -> List[SMS]:
    async with sem:
        result = await asyncio.to_thread(
            call_llm,
            prompt_id="extract_sms",
            input=raw_text,
            output_format=SMSList,
            model=os.getenv("EXTRACTION_MODEL")
        )
        return result.messages

async def extract_sms_parallel(sms_json_path: str, max_concurrent: int = 5, max_rows: Optional[int] = None) -> List[SMS]:
    raw_data = json.loads(read_file(sms_json_path))
    
    if max_rows is not None:
        raw_data = raw_data[:max_rows]
        
    sem = asyncio.Semaphore(max_concurrent)
    
    tasks = [
        process_sms(entry.get("sms", ""), sem)
        for entry in raw_data if entry.get("sms")
    ]
    
    nested_results = await asyncio.gather(*tasks)
    flat_results = [sms for sublist in nested_results for sms in sublist]
    
    return flat_results