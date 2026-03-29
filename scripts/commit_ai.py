#!/usr/bin/env python3
"""
commit_ai.py

Generate a git commit message from the current diff using the OpenAI Responses API,
then optionally create the commit.

Typical usage:
    python scripts/commit_ai.py
    python scripts/commit_ai.py --all
    python scripts/commit_ai.py --dry-run
    python scripts/commit_ai.py --body
    python scripts/commit_ai.py --model gpt-5.4-mini

Behavior:
- Uses staged changes by default.
- With --all, stages tracked modifications/deletions via `git add -u`.
- With --include-untracked, also stages new files via `git add --intent-to-add .`
  before reading the diff, so untracked file names appear in the diff.
- Sends a truncated diff to the OpenAI Responses API and asks for JSON:
    {"subject": "...", "body": ["...", "..."]}
- Creates a commit with the generated message unless --dry-run is passed.

Environment:
- Requires OPENAI_API_KEY
- Optional OPENAI_BASE_URL for compatible gateways
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

try:
    import requests
except ImportError as exc:
    raise SystemExit(
        "This script requires the 'requests' package. Install it with: pip install requests"
    ) from exc


DEFAULT_MODEL = "gpt-5.4-mini"
DEFAULT_MAX_DIFF_CHARS = 120_000
COMMIT_SUBJECT_LIMIT = 72


def run_git(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        check=check,
        text=True,
        capture_output=True,
    )


def ensure_git_repo() -> None:
    result = run_git("rev-parse", "--is-inside-work-tree")
    if result.stdout.strip() != "true":
        raise SystemExit("Not inside a git working tree.")


def maybe_stage(all_tracked: bool, include_untracked: bool) -> None:
    if all_tracked:
        run_git("add", "-u")
    if include_untracked:
        run_git("add", "--intent-to-add", ".")


def get_staged_diff(max_chars: int) -> str:
    result = run_git("diff", "--cached", "--no-color", "--binary", "--minimal")
    diff = result.stdout
    if not diff.strip():
        return ""
    if len(diff) <= max_chars:
        return diff
    head = diff[: max_chars - 200]
    tail_note = f"\n\n[DIFF TRUNCATED TO {max_chars} CHARS]\n"
    return head + tail_note


def get_changed_files() -> list[str]:
    result = run_git("diff", "--cached", "--name-only", "--diff-filter=ACDMRTUXB")
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def sanitize_subject(subject: str) -> str:
    subject = " ".join(subject.strip().split())
    subject = subject.rstrip(".")
    if len(subject) > COMMIT_SUBJECT_LIMIT:
        subject = subject[: COMMIT_SUBJECT_LIMIT].rstrip()
    if not subject:
        subject = "Update repository changes"
    return subject


def sanitize_body(lines: list[str]) -> list[str]:
    cleaned: list[str] = []
    for line in lines:
        text = " ".join(str(line).strip().split())
        if not text:
            continue
        if text.startswith("- "):
            text = text[2:].strip()
        cleaned.append(text)
    return cleaned[:5]


def heuristic_message(files: list[str]) -> dict[str, Any]:
    if not files:
        return {"subject": "Update repository changes", "body": []}

    top = files[:4]
    if len(files) == 1:
        subject = f"Update {Path(files[0]).name}"
    else:
        subject = f"Update {len(files)} files"
    body = [f"Touch {name}" for name in top]
    if len(files) > len(top):
        body.append(f"Plus {len(files) - len(top)} more file changes")
    return {"subject": sanitize_subject(subject), "body": sanitize_body(body)}


def build_prompt(diff: str, files: list[str], body: bool) -> str:
    file_list = "\n".join(f"- {name}" for name in files[:100]) or "- (none detected)"
    body_rules = (
        "Include 1-4 short bullet items in `body`."
        if body
        else "Return an empty `body` list unless a short body is truly useful."
    )
    return f"""You are writing a git commit message.

Return JSON only, with this exact shape:
{{
  "subject": "string",
  "body": ["string", "string"]
}}

Rules:
- `subject` must be imperative mood.
- `subject` must be concise and <= {COMMIT_SUBJECT_LIMIT} characters.
- No conventional commit prefixes unless the diff clearly demands one.
- No trailing period in the subject.
- No markdown fences.
- {body_rules}
- Body items, if any, must be short fragments, not paragraphs.
- Focus on what changed, not why an AI is involved.

Changed files:
{file_list}

Git diff:
{diff}
"""


def extract_output_text(payload: dict[str, Any]) -> str:
    if isinstance(payload.get("output_text"), str) and payload["output_text"].strip():
        return payload["output_text"]

    output = payload.get("output", [])
    texts: list[str] = []
    for item in output:
        for content in item.get("content", []):
            text = content.get("text")
            if isinstance(text, str):
                texts.append(text)
    return "\n".join(texts).strip()


def call_openai(prompt: str, model: str) -> dict[str, Any]:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise SystemExit("OPENAI_API_KEY is not set.")

    base_url = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
    url = f"{base_url}/responses"

    payload = {
        "model": model,
        "input": [
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": prompt},
                ],
            }
        ],
        "text": {
            "verbosity": "low",
        },
    }

    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=120,
    )
    resp.raise_for_status()
    data = resp.json()
    text = extract_output_text(data)

    if not text:
        raise RuntimeError("Model returned no text output.")

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not match:
            raise RuntimeError(f"Model output was not valid JSON:\n{text}") from None
        parsed = json.loads(match.group(0))

    if not isinstance(parsed, dict):
        raise RuntimeError("Model output JSON was not an object.")

    subject = sanitize_subject(str(parsed.get("subject", "")))
    body = sanitize_body(list(parsed.get("body", []))) if isinstance(parsed.get("body"), list) else []

    return {"subject": subject, "body": body}


def build_commit_message(subject: str, body: list[str]) -> str:
    if not body:
        return subject
    bullet_lines = [f"- {line}" for line in body]
    return subject + "\n\n" + "\n".join(bullet_lines)


def create_commit(message: str, no_verify: bool) -> None:
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as handle:
        handle.write(message)
        temp_path = handle.name
    try:
        cmd = ["git", "commit", "-F", temp_path]
        if no_verify:
            cmd.append("--no-verify")
        subprocess.run(cmd, check=True)
    finally:
        try:
            os.remove(temp_path)
        except OSError:
            pass


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate a commit message from the current git diff.")
    parser.add_argument("--all", action="store_true", help="Stage tracked modifications/deletions before reading the diff.")
    parser.add_argument(
        "--include-untracked",
        action="store_true",
        help="Surface untracked files in the diff by marking them intent-to-add before reading the diff.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print the generated commit message and exit.")
    parser.add_argument("--body", action="store_true", help="Ask for a short commit body as well.")
    parser.add_argument("--no-verify", action="store_true", help="Pass --no-verify to git commit.")
    parser.add_argument("--model", default=DEFAULT_MODEL, help=f"Responses API model to use (default: {DEFAULT_MODEL}).")
    parser.add_argument(
        "--max-diff-chars",
        type=int,
        default=DEFAULT_MAX_DIFF_CHARS,
        help=f"Maximum diff characters to send to the API (default: {DEFAULT_MAX_DIFF_CHARS}).",
    )
    args = parser.parse_args()

    ensure_git_repo()
    maybe_stage(args.all, args.include_untracked)

    diff = get_staged_diff(args.max_diff_chars)
    files = get_changed_files()

    if not diff.strip():
        print("No staged diff found. Stage files first, or run with --all.", file=sys.stderr)
        return 2

    try:
        result = call_openai(build_prompt(diff, files, args.body), model=args.model)
    except Exception as exc:
        print(f"OpenAI call failed, falling back to heuristic message: {exc}", file=sys.stderr)
        result = heuristic_message(files)

    subject = sanitize_subject(result["subject"])
    body = sanitize_body(result.get("body", []))
    message = build_commit_message(subject, body)

    print("\nGenerated commit message:\n")
    print(message)
    print()

    if args.dry_run:
        return 0

    create_commit(message, no_verify=args.no_verify)
    print("Commit created.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
