import os
import ulid
from pydantic import BaseModel
from langfuse.openai import OpenAI
from langfuse.decorators import observe
from read_prompt import get_prompt

client = OpenAI(
    api_key=os.getenv("OPENROUTER_API_KEY"),
    base_url="https://openrouter.ai/api/v1"
)

def generate_session_id() -> str:
    team = os.getenv("TEAM_NAME", "tutorial").replace(" ", "-")
    return f"{team}-{ulid.new().str}"

@observe()
def call_llm(prompt_id: str, model: str, input: str, output_format: type[BaseModel], session_id: str = None) -> BaseModel:
    prompt_text = get_prompt(prompt_id)
    active_session_id = session_id or generate_session_id()

    response = client.beta.chat.completions.parse(
        model=model,
        messages=[
            {"role": "system", "content": prompt_text},
            {"role": "user", "content": input}
        ],
        response_format=output_format,
        session_id=active_session_id
    )

    return response.choices[0].message.parsed