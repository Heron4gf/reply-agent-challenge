import os
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

def generate_session_id() -> str:
    team = os.getenv("TEAM_NAME", "tutorial").replace(" ", "-")
    return f"{team}-{ulid.new().str}"

@observe()
def call_llm(
    prompt_id: str,
    input: str,
    output_format: type[BaseModel],
    model: str = "openai/gpt-4o-mini",
    session_id: str | None = None
) -> BaseModel:
    prompt_text = get_prompt(prompt_id)
    active_session_id = session_id or generate_session_id()

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