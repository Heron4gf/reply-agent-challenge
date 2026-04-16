import os
import ulid
from pydantic import BaseModel
from openai import OpenAI
from openai.lib._parsing import type_to_response_format_param
from langfuse import observe, propagate_attributes
from utils.read_prompt import get_prompt
from dotenv import load_dotenv

load_dotenv()

client = OpenAI(
    api_key=os.getenv("OPENROUTER_API_KEY"),
    base_url="https://openrouter.ai/api/v1"
)

def generate_session_id() -> str:
    team = os.getenv("TEAM_NAME", "tutorial").replace(" ", "-")
    return f"{team}-{ulid.new().str}"

@observe()
def call_llm(
    prompt_id: str,
    model: str,
    input: str,
    output_format: type[BaseModel],
    session_id: str | None = None
) -> BaseModel:
    prompt_text = get_prompt(prompt_id)
    active_session_id = session_id or generate_session_id()

    with propagate_attributes(session_id=active_session_id):
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": prompt_text},
                {"role": "user", "content": input},
            ],
            temperature=0.1,
            response_format=type_to_response_format_param(output_format),
        )

    content = response.choices[0].message.content
    return output_format.model_validate_json(content)