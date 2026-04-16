import json
import os
import asyncio
from typing import List, Optional
from utils.call_llm import call_llm
from utils.read_prompt import read_file
from models.input_data import Email, EmailList

async def process_email(raw_text: str, sem: asyncio.Semaphore) -> List[Email]:
    async with sem:
        result = await asyncio.to_thread(
            call_llm,
            prompt_id="extract_emails",
            input=raw_text,
            output_format=EmailList,
            model=os.getenv("EXTRACTION_MODEL")
        )
        return result.emails

async def extract_emails_parallel(email_json_path: str, max_concurrent: int = 5, max_rows: Optional[int] = None) -> List[Email]:
    raw_data = json.loads(read_file(email_json_path))
    
    if max_rows is not None:
        raw_data = raw_data[:max_rows]
        
    sem = asyncio.Semaphore(max_concurrent)
    
    tasks = [
        process_email(entry.get("mail", ""), sem)
        for entry in raw_data if entry.get("mail")
    ]
    
    nested_results = await asyncio.gather(*tasks)
    flat_results = [email for sublist in nested_results for email in sublist]
    
    return flat_results