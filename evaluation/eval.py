import asyncio
import csv
from pathlib import Path
from difflib import SequenceMatcher

from pydantic_ai import Agent


PROMPT_PATH   = Path("./prompt.md")
DATASET_PATH  = Path("./dataset.csv")
MODELS_PATH   = Path("./eval_models.csv")
RESULTS_PATH  = Path("./eval_results.csv")

MAX_CONCURRENT = 5  # concurrent case runs per model


# ── Loaders ──────────────────────────────────────────────────────────────────

def load_prompt() -> str:
    return PROMPT_PATH.read_text(encoding="utf-8").strip()


def load_dataset() -> list[dict[str, str]]:
    with DATASET_PATH.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))  # [{question, expected_response}, ...]


def load_models() -> list[str]:
    with MODELS_PATH.open(newline="", encoding="utf-8") as f:
        return [row["model_name"] for row in csv.DictReader(f)]


# ── Scoring ───────────────────────────────────────────────────────────────────

def similarity(output: str, expected: str) -> float:
    """Normalised similarity in [0, 1]. Replace with an LLM judge if needed."""
    return SequenceMatcher(
        None,
        output.strip().lower(),
        expected.strip().lower(),
    ).ratio()


# ── Evaluation ────────────────────────────────────────────────────────────────

async def run_case(
    agent: Agent,
    model_name: str,
    case: dict[str, str],
    sem: asyncio.Semaphore,
) -> float:
    async with sem:
        try:
            result = await agent.run(case["question"])
            score = similarity(result.output, case["expected_response"])
        except Exception as exc:
            print(f"  ✗ [{model_name}] {case['question'][:60]!r} → ERROR: {exc}")
            score = 0.0
        else:
            print(f"  {'✓' if score >= 0.5 else '~'} [{model_name}] "
                  f"{case['question'][:60]!r} → {score:.3f}")
        return score


async def eval_model(
    model_name: str,
    dataset: list[dict[str, str]],
    system_prompt: str,
) -> float:
    agent = Agent(model=model_name, instructions=system_prompt)
    sem   = asyncio.Semaphore(MAX_CONCURRENT)

    scores = await asyncio.gather(
        *[run_case(agent, model_name, case, sem) for case in dataset]
    )
    return sum(scores) / len(scores) if scores else 0.0


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    system_prompt = load_prompt()
    dataset       = load_dataset()
    models        = load_models()

    print(f"Models: {len(models)}  |  Cases: {len(dataset)}\n{'─' * 60}")

    results: list[dict] = []

    for model_name in models:
        print(f"\n▶ {model_name}")
        avg_score = await eval_model(model_name, dataset, system_prompt)
        print(f"  avg score: {avg_score:.4f}")
        results.append({"model_name": model_name, "score": round(avg_score, 4)})

    # ── Write results ─────────────────────────────────────────────────────────
    with RESULTS_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["model_name", "score"])
        writer.writeheader()
        writer.writerows(results)

    # ── Leaderboard ───────────────────────────────────────────────────────────
    print(f"\n{'─' * 60}\n  LEADERBOARD")
    for rank, r in enumerate(
        sorted(results, key=lambda x: x["score"], reverse=True), 1
    ):
        print(f"  {rank}. {r['model_name']:<40} {r['score']:.4f}")

    print(f"\nResults saved → {RESULTS_PATH}")


if __name__ == "__main__":
    asyncio.run(main())