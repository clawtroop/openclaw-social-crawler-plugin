from __future__ import annotations

import sys
from pathlib import Path


def resolve_crawler_root() -> Path:
    import os

    root = os.environ.get("SOCIAL_CRAWLER_ROOT")
    if not root:
        raise RuntimeError("SOCIAL_CRAWLER_ROOT is required")
    path = Path(root).resolve()
    if not path.exists():
        raise RuntimeError(f"SOCIAL_CRAWLER_ROOT does not exist: {path}")
    return path


def inject_crawler_root() -> Path:
    root = resolve_crawler_root()
    root_str = str(root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)
    return root


def resolve_wallet_config() -> tuple[str, str]:
    """Return ``(wallet_bin, wallet_token)`` from environment variables.

    * ``AWP_WALLET_BIN``   – path to awp-wallet CLI (default ``"awp-wallet"``)
    * ``AWP_WALLET_TOKEN`` – session token from ``awp-wallet unlock --scope full``
    """
    import os

    return (
        os.environ.get("AWP_WALLET_BIN", "awp-wallet"),
        os.environ.get("AWP_WALLET_TOKEN", ""),
    )
