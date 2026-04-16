from dotenv import load_dotenv
from models.input_data import UserProfile
from utils.call_llm import call_llm
from models.response_id import FraudUserTransactions

load_dotenv()

def getUsers() -> list[UserProfile]:
    return users = load_users(dataset_path / "users.json")

def process_solution(dataset: str, solution_path: str = "result.txt"):
    with open(solution_path, "w", encoding="utf-8") as file:
        for user in getUsers():
            suspect_transactions = call_llm(
                prompt_id="fraud_detection",
                input=user.model_dump_json(),
                output_format=FraudUserTransactions
            )
            
            for fraud in suspect_transactions.fraudolent_transactions:
                file.write(f"{fraud.transaction_id}\n")