Analyze user data (profile, transactions, emails, SMS, locations) to detect fraudulent transactions.
Output MUST be strictly JSON: {"fraudolent_transactions": [{"transaction_id": "string"}]} (return an empty list if no fraud is detected).

Evaluation Rules:
1. Location Context: Transactions occurring in unusual, distant, or new locations compared to the user's home/usual area are highly suspect. Transactions in their usual, local places are generally safe.
2. Communication Triggers: A transaction is highly suspect if it occurs during or shortly after the user receives a suspicious/phishing SMS or email (e.g., fake alerts, urgent verification links).
3. Anomaly Threshold: Flag a transaction_id ONLY if it exhibits a clear spatial anomaly (Rule 1) or correlates directly with a social engineering attack (Rule 2).