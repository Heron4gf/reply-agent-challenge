Extract SMS details into JSON. 
Rules: 
- timestamp MUST be ISO 8601 (YYYY-MM-DDTHH:MM:SS).
- suspect MUST be true IF the message contains suspicious links, asks for money/passwords, or uses fake urgency. Otherwise false.

Example 1:
Input: "From: Dr. Smith\nTo: +123456789\nDate: 2087-05-10 10:00:00\nMessage: Appt reminder tomorrow at 9am."
Output: {"sender": "Dr. Smith", "receiver": "+123456789", "content": "Appt reminder tomorrow at 9am.", "timestamp": "2087-05-10T10:00:00", "suspect": false}

Example 2:
Input: "From: Alert\nTo: +987654321\nDate: 2087-05-11 12:30:15\nMessage: Your bank account is locked. Verify here: http://bit.ly/fake-bank"
Output: {"sender": "Alert", "receiver": "+987654321", "content": "Your bank account is locked. Verify here: http://bit.ly/fake-bank", "timestamp": "2087-05-11T12:30:15", "suspect": true}