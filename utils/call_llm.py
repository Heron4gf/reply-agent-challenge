import os
import json
import ulid
from pydantic import BaseModel
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from langfuse import observe
from langfuse.langchain import CallbackHandler
from langfuse import Langfuse
from utils.read_prompt import get_prompt

load_dotenv()

langfuse_client = Langfuse(
    public_key=os.getenv("LANGFUSE_PUBLIC_KEY"),
    secret_key=os.getenv("LANGFUSE_SECRET_KEY"),
    host=os.getenv("LANGFUSE_HOST", "https://challenges.reply.com/langfuse")
)

SESSION_CACHE_FILE = "session_id.json"

def generate_session_id() -> str:
    team = os.getenv("TEAM_NAME", "tutorial").replace(" ", "-")
    return f"{team}-{ulid.new().str}"

def get_cached_session_id(cache_file: str = SESSION_CACHE_FILE) -> str:
    if not os.path.exists(cache_file):
        session_id = generate_session_id()
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump({"session_id": session_id}, f)
        return session_id

    try:
        with open(cache_file, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        raise RuntimeError(
            f"Cannot read session cache file '{cache_file}'. "
            "File exists but is unreadable or contains invalid JSON."
        ) from e

    session_id = data.get("session_id")
    if not isinstance(session_id, str) or not session_id.strip():
        raise RuntimeError(
            f"Session cache file '{cache_file}' is valid JSON but does not contain a usable 'session_id'."
        )

    return session_id

@observe()
def call_llm(
    prompt_id: str,
    input: str,
    output_format: type[BaseModel],
    model: str,
    session_id: str | None = None
) -> BaseModel:
    prompt_text = get_prompt(prompt_id)
    active_session_id = session_id or get_cached_session_id()

    llm = ChatOpenAI(
        api_key=os.getenv("OPENROUTER_API_KEY"),
        base_url="https://openrouter.ai/api/v1",
        model=model,
        temperature=0.1,
    )

    structured_llm = llm.with_structured_output(output_format)
    langfuse_handler = CallbackHandler()

    messages = [
        SystemMessage(content=prompt_text),
        HumanMessage(content=input)
    ]

    response = structured_llm.invoke(
        messages,
        config={
            "callbacks": [langfuse_handler],
            "metadata": {"langfuse_session_id": active_session_id},
        }
    )

    return response