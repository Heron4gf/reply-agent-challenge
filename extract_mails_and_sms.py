import asyncio
import os
from extract.extract_emails import extract_emails_parallel
from extract.extract_messages import extract_sms_parallel
from utils.save_extracted_results import save_results

def data_path(*parts):
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", *parts)

async def main():
    base_dir = data_path("The Truman Show - train")
    
    mails_input = os.path.join(base_dir, "mails.json")
    mails_output = os.path.join(base_dir, "mails_processed.json")
    
    sms_input = os.path.join(base_dir, "sms.json")
    sms_output = os.path.join(base_dir, "sms_processed.json")

    print(f"Processing up to 5 raw email threads from {mails_input}...")
    emails = await extract_emails_parallel(
        email_json_path=mails_input,
        max_concurrent=4
    )
    save_results(emails, mails_output)
    print(f"Saved {len(emails)} individual emails to {mails_output}")

    print(f"Processing up to 5 raw SMS threads from {sms_input}...")
    sms_list = await extract_sms_parallel(
        sms_json_path=sms_input,
        max_concurrent=4
    )
    save_results(sms_list, sms_output)
    print(f"Saved {len(sms_list)} individual SMS to {sms_output}")

if __name__ == "__main__":
    asyncio.run(main())