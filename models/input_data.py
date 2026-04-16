from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional

class Location(BaseModel):
    lat: float
    lng: float
    city: str

class DeviceLocation(BaseModel):
    location: Location
    biotag: str
    timestamp: datetime

class SMS(BaseModel):
    sender: str
    receiver: str
    content: str
    timestamp: datetime
    suspect: bool

class SMSList(BaseModel):
    messages: List[SMS]

class Email(BaseModel):
    sender_email: str
    sender_name: str
    receiver_email: str
    receiver_name: str
    content: str
    timestamp: datetime
    suspect: bool

class EmailList(BaseModel):
    emails: List[Email]

class Transaction(BaseModel):
    transaction_id: str
    sender: DeviceLocation
    recipient: DeviceLocation
    transaction_type: str
    amount: float
    location: Optional[str] = None
    payment_method: Optional[str] = None
    sender_iban: str
    recipient_iban: str
    balance_after: float
    description: str
    timestamp: datetime

class UserProfile(BaseModel):
    first_name: str
    last_name: str
    birth_year: int
    salary: float
    job: str
    iban: str
    residence: Location
    description: str

    # linked data
    locations: List[Location] = []
    sms_chats: List[SMS] = []
    emails: List[Email] = []
    transactions: List[Transaction] = []