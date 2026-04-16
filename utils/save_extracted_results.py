import json
from typing import List
from pydantic import BaseModel

def save_results(models: List[BaseModel], file_path: str):
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump([model.model_dump(mode='json') for model in models], file, indent=4)