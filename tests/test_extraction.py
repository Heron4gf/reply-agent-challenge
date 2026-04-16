import asyncio
import os
from extract.extract_emails import extract_emails_parallel
from extract.extract_messages import extract_sms_parallel
from utils.save_extracted_results import save_results
from dotenv import load_dotenv

load_dotenv()

async def main():
    base_dir = os.path.join("data", "The Truman Show - train")
    
    mails_input = os.path.join(base_dir, "mails.json")
    mails_output = os.path.join(base_dir, "mails_processed.json")
    
    sms_input = os.path.join(base_dir, "sms.json")
    sms_output = os.path.join(base_dir, "sms_processed.json")

    print(f"Processing up to 5 emails from {mails_input}...")
    emails = await extract_emails_parallel(
        email_json_path=mails_input,
        max_concurrent=4,
        max_rows=5
    )
    save_results(emails, mails_output)
    print(f"Saved {len(emails)} emails to {mails_output}")

    print(f"Processing up to 5 SMS from {sms_input}...")
    sms_list = await extract_sms_parallel(
        sms_json_path=sms_input,
        max_concurrent=4,
        max_rows=5
    )
    save_results(sms_list, sms_output)
    print(f"Saved {len(sms_list)} SMS to {sms_output}")

if __name__ == "__main__":
    asyncio.run(main())