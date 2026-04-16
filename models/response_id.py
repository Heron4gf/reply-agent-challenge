from pydantic import BaseModel
from typing import List

class FraudolentTransaction(BaseModel):
    transaction_id: str

class FraudUserTransactions(BaseModel):
    fraudolent_transactions: List[FraudolentTransaction]