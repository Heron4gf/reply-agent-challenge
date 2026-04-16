import json
from typing import List
from pydantic import BaseModel
from utils.call_llm import call_llm
from utils.read_prompt import read_file
from models.input_data import SMS

class RawSMS(BaseModel):
    sms: str

def extract_sms(sms_json_path: str) -> List[SMS]:
    raw_data = json.loads(read_file(sms_json_path))
    
    extracted_messages = []
    
    for entry in raw_data:
        raw_text = entry.get("sms", "")
        if raw_text:
            parsed_sms = call_llm(
                prompt_id="extract_sms",
                input=raw_text,
                output_format=SMS
            )
            extracted_messages.append(parsed_sms)
            
    return extracted_messages