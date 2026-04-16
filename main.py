import os
from dotenv import load_dotenv
from utils.call_llm import call_llm
from models.response_id import FraudUserTransactions
from typing import List
from utils.read_prompt import read_file

load_dotenv()

def getUsers(path: str = "output/enriched_users") -> List[str]:
    if not os.path.exists(path):
        return []
    return [os.path.join(path, f) for f in os.listdir(path) if f.endswith('.json')]

def getUser(path: str) -> str:
    return read_file(path)

def process_solution(solution_path: str = "result.txt"):
    with open(solution_path, "w", encoding="utf-8") as file:
        for user_path in getUsers():
            user_data = getUser(user_path)
            
            suspect_transactions = call_llm(
                prompt_id="fraud_detection",
                input=user_data,
                output_format=FraudUserTransactions
            )
            
            if suspect_transactions and suspect_transactions.fraudolent_transactions:
                for fraud in suspect_transactions.fraudolent_transactions:
                    file.write(f"{fraud.transaction_id}\n")

if __name__ == "__main__":
    process_solution()