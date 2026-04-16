Extract email details into JSON.
Rules:
- timestamp MUST be ISO 8601. Convert RFC 2822 dates.
- Clean ALL HTML tags from the content, leaving only plain text.
- suspect MUST be true IF the email is phishing, requests urgent payment, or has sender/link mismatches. Otherwise false.

Example 1:
Input: "From: \"Bob\" <bob@work.com>\nTo: \"Alice\" <alice@work.com>\nDate: Mon, 01 Jan 2087 09:00:00 +0000\nSubject: Meeting\n\n<html><body>Let's meet at 10.</body></html>"
Output: {"sender_email": "bob@work.com", "sender_name": "Bob", "receiver_email": "alice@work.com", "receiver_name": "Alice", "content": "Let's meet at 10.", "timestamp": "2087-01-01T09:00:00", "suspect": false}

Example 2:
Input: "From: \"PayPa1 Sec\" <scam@bad.net>\nTo: \"Alice\" <alice@work.com>\nDate: Tue, 02 Jan 2087 14:22:10 +0000\nSubject: URGENT\n\nYour account is restricted. Click here to verify."
Output: {"sender_email": "scam@bad.net", "sender_name": "PayPa1 Sec", "receiver_email": "alice@work.com", "receiver_name": "Alice", "content": "Your account is restricted. Click here to verify.", "timestamp": "2087-01-02T14:22:10", "suspect": true}