import os

# simple in‑memory cache
_PROMPT_CACHE = {}


def read_file(path: str) -> str:
    """Read a file and return its contents as a string."""
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def read_prompt(prompt_name: str) -> str:
    """
    Load a Markdown prompt from ../prompts/.
    Example: read_prompt("system") → loads ../prompts/system.md
    """
    base_dir = os.path.join(os.path.dirname(__file__), "..", "prompts")

    filename = prompt_name if prompt_name.endswith(".md") else f"{prompt_name}.md"
    path = os.path.join(base_dir, filename)

    return read_file(path)


def get_prompt(prompt_name: str) -> str:
    """
    Cached prompt loader.
    Returns the prompt from memory if already loaded,
    otherwise loads it from disk and caches it.
    """
    if prompt_name in _PROMPT_CACHE:
        return _PROMPT_CACHE[prompt_name]

    prompt = read_prompt(prompt_name)
    _PROMPT_CACHE[prompt_name] = prompt
    return prompt