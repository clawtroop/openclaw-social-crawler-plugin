from __future__ import annotations

from typing import Any


class UnsupportedChallenge(RuntimeError):
    def __init__(self, challenge_type: str) -> None:
        super().__init__(f"unsupported challenge type: {challenge_type}")


def solve_challenge(challenge: dict[str, Any]) -> str:
    challenge_type = str(challenge.get("question_type") or "unknown")
    if challenge_type == "content_understanding":
        return "accepted"
    raise UnsupportedChallenge(challenge_type)
