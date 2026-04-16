import json
import asyncio
from typing import List
from datetime import datetime
from pydantic import BaseModel
from utils.call_llm import call_llm
from utils.read_prompt import read_file

class Email(BaseModel):
    sender_email: str
    sender_name: str
    receiver_email: str
    receiver_name: str
    content: str
    timestamp: datetime
    suspect: bool

async def process_email(raw_text: str, sem: asyncio.Semaphore) -> Email:
    async with sem:
        return await asyncio.to_thread(
            call_llm,
            prompt_id="extract_emails",
            input=raw_text,
            output_format=Email
        )

async def extract_emails_parallel(email_json_path: str, max_concurrent: int = 5) -> List[Email]:
    raw_data = json.loads(read_file(email_json_path))
    sem = asyncio.Semaphore(max_concurrent)
    
    tasks = [
        process_email(entry.get("mail", ""), sem)
        for entry in raw_data if entry.get("mail")
    ]
    
    return await asyncio.gather(*tasks)