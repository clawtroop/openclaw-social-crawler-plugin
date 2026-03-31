"""EIP-712 signing via awp-wallet CLI subprocess."""

from __future__ import annotations

import json
import subprocess
import time
from typing import Any


class WalletSigner:
    """Bridge to awp-wallet CLI for EIP-712 request signing.

    Private keys never enter Python process memory — all signing is
    delegated to the awp-wallet subprocess.
    """

    def __init__(self, wallet_bin: str = "awp-wallet", session_token: str = "") -> None:
        self._bin = wallet_bin
        self._token = session_token
        self._signer_address: str | None = None

    def _run(self, *args: str) -> dict[str, Any]:
        """Run awp-wallet CLI command, return parsed JSON."""
        cmd = [self._bin, *args]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            raise RuntimeError(f"awp-wallet failed (exit {result.returncode}): {result.stderr.strip()}")
        return json.loads(result.stdout)

    def get_address(self) -> str:
        """Get signer address (cached).  No session token needed."""
        if self._signer_address is None:
            resp = self._run("receive")
            addr = resp.get("address") or ""
            if not addr:
                addresses = resp.get("addresses")
                if isinstance(addresses, list) and addresses:
                    first = addresses[0]
                    addr = first.get("address", "") if isinstance(first, dict) else ""
            if not addr:
                raise RuntimeError("awp-wallet receive did not return an address")
            self._signer_address = addr
        return self._signer_address

    def sign_typed_data(self, typed_data: dict[str, Any]) -> str:
        """Sign EIP-712 typed data, return hex signature."""
        resp = self._run(
            "sign-typed-data",
            "--token", self._token,
            "--data", json.dumps(typed_data, separators=(",", ":")),
        )
        sig = resp.get("signature", "")
        if not sig:
            raise RuntimeError("awp-wallet sign-typed-data returned empty signature")
        return sig

    def build_auth_headers(
        self,
        method: str,
        path: str,
        body: str | None = None,
    ) -> dict[str, str]:
        """Build EIP-712 auth headers for a Platform Service request.

        Header format (verified against backend):
          X-Signer     — Ethereum address
          X-Nonce      — Unix timestamp (seconds), must be numeric
          X-Issued-At  — Unix timestamp (seconds)
          X-Expires-At — Unix timestamp (seconds), validity ≤ 60s
          X-Signature  — EIP-712 signature over the above fields
        """
        signer = self.get_address()
        now = int(time.time())
        nonce = str(now)
        issued_at = str(now)
        expires_at = str(now + 60)

        typed_data: dict[str, Any] = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                ],
                "Request": [
                    {"name": "signer", "type": "address"},
                    {"name": "nonce", "type": "string"},
                    {"name": "issued_at", "type": "string"},
                    {"name": "expires_at", "type": "string"},
                ],
            },
            "primaryType": "Request",
            "domain": {"name": "PlatformService", "version": "1"},
            "message": {
                "signer": signer,
                "nonce": nonce,
                "issued_at": issued_at,
                "expires_at": expires_at,
            },
        }
        signature = self.sign_typed_data(typed_data)
        return {
            "X-Signer": signer,
            "X-Signature": signature,
            "X-Nonce": nonce,
            "X-Issued-At": issued_at,
            "X-Expires-At": expires_at,
        }
