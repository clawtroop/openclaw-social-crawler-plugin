from __future__ import annotations

from pow_solver import UnsupportedChallenge, solve_challenge


def test_content_understanding_returns_accepted() -> None:
    answer = solve_challenge({"question_type": "content_understanding"})

    assert answer == "accepted"


def test_unknown_challenge_stays_explicit() -> None:
    try:
        solve_challenge({"question_type": "unknown"})
    except UnsupportedChallenge as error:
        assert str(error) == "unsupported challenge type: unknown"
    else:
        raise AssertionError("expected UnsupportedChallenge")
