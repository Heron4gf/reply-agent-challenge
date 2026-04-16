import os
import asyncio
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

async def process_user(user_path: str, sem: asyncio.Semaphore, index: int, total: int) -> List[str]:
    async with sem:
        print(f"[{index}/{total}] Started processing: {os.path.basename(user_path)}")
        user_data = await asyncio.to_thread(getUser, user_path)
        
        suspect_transactions = await asyncio.to_thread(
            call_llm,
            prompt_id="fraud_detection",
            input=user_data,
            output_format=FraudUserTransactions,
            model=os.getenv("JUDGE_MODEL")
        )
        
        print(f"[{index}/{total}] Completed: {os.path.basename(user_path)}")
        
        if suspect_transactions and suspect_transactions.fraudolent_transactions:
            return [fraud.transaction_id for fraud in suspect_transactions.fraudolent_transactions]
        return []

async def process_solution(solution_path: str = "result.txt", max_concurrent: int = 5):
    users = getUsers()
    total_users = len(users)
    
    if total_users == 0:
        print("No user profiles found.")
        return

    print(f"Initiating fraud detection for {total_users} users (concurrency: {max_concurrent})...")
    sem = asyncio.Semaphore(max_concurrent)
    
    tasks = [
        process_user(user_path, sem, i + 1, total_users)
        for i, user_path in enumerate(users)
    ]
    
    nested_results = await asyncio.gather(*tasks)
    flat_results = [tx_id for sublist in nested_results for tx_id in sublist]
    
    with open(solution_path, "w", encoding="utf-8") as file:
        for tx_id in flat_results:
            file.write(f"{tx_id}\n")
            
    print(f"\nExecution finished. Exported {len(flat_results)} suspicious transaction IDs to {solution_path}.")

if __name__ == "__main__":
    asyncio.run(process_solution())