"""EIP-712 signing via awp-wallet CLI subprocess."""

from __future__ import annotations

import json
import os
import secrets
import subprocess
import time
from typing import Any
from urllib.parse import parse_qsl, quote, urlsplit

from Crypto.Hash import keccak

EMPTY_HASH = f"0x{'0' * 64}"
DEFAULT_SIGNED_HEADERS = ("content-type", "x-request-id")


def _normalize_header_value(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _keccak_hex(text: str) -> str:
    if not text:
        return EMPTY_HASH
    digest = keccak.new(digest_bits=256)
    digest.update(text.encode("utf-8"))
    return "0x" + digest.hexdigest()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash_query(url: str) -> str:
    split = urlsplit(url)
    pairs = []
    for key, value in parse_qsl(split.query, keep_blank_values=True):
        pairs.append((quote(key, safe=""), quote(value, safe="")))
    if not pairs:
        return EMPTY_HASH
    pairs.sort()
    return _keccak_hex("&".join(f"{key}={value}" for key, value in pairs))


def _hash_headers(headers: dict[str, str], signed_headers: tuple[str, ...]) -> str:
    lines = []
    for header_name in sorted(signed_headers):
        value = headers.get(header_name)
        if value is None:
            continue
        lines.append(f"{header_name}:{_normalize_header_value(value)}")
    if not lines:
        return EMPTY_HASH
    return _keccak_hex("\n".join(lines))


def _canonical_body(body: Any, content_type: str) -> str | None:
    if body is None:
        return None
    normalized_type = str(content_type or "").lower()
    if "application/json" in normalized_type:
        return _canonical_json(body)
    if isinstance(body, str):
        return body
    return json.dumps(body, ensure_ascii=False)


def _hash_body(body: Any, content_type: str) -> str:
    canonical_body = _canonical_body(body, content_type)
    if not canonical_body:
        return EMPTY_HASH
    return _keccak_hex(canonical_body)


class WalletSigner:
    """Bridge to awp-wallet CLI for EIP-712 request signing."""

    def __init__(self, wallet_bin: str = "awp-wallet", session_token: str = "") -> None:
        self._bin = wallet_bin
        self._token = session_token
        self._signer_address: str | None = None

    def _run(self, *args: str) -> dict[str, Any]:
        cmd = [self._bin, *args]
        env = os.environ.copy()
        if not env.get("HOME") and env.get("USERPROFILE"):
            env["HOME"] = env["USERPROFILE"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, env=env)
        if result.returncode != 0:
            raise RuntimeError(f"awp-wallet failed (exit {result.returncode}): {result.stderr.strip()}")
        return json.loads(result.stdout)

    def get_address(self) -> str:
        if self._signer_address is None:
            resp = self._run("receive")
            addr = resp.get("address") or resp.get("eoaAddress") or ""
            if not addr:
                addresses = resp.get("addresses")
                if isinstance(addresses, list) and addresses:
                    first = addresses[0]
                    if isinstance(first, dict):
                        addr = first.get("address", "") or first.get("eoaAddress", "")
            if not addr:
                raise RuntimeError("awp-wallet receive did not return an address")
            self._signer_address = addr
        return self._signer_address

    def sign_typed_data(self, typed_data: dict[str, Any]) -> str:
        resp = self._run(
            "sign-typed-data",
            "--token",
            self._token,
            "--data",
            json.dumps(typed_data, separators=(",", ":")),
        )
        sig = resp.get("signature", "")
        if not sig:
            raise RuntimeError("awp-wallet sign-typed-data returned empty signature")
        return sig

    def build_typed_data(
        self,
        *,
        method: str,
        url: str,
        body: Any,
        content_type: str,
        request_id: str,
        now: int,
        nonce: int,
        chain_id: int = 1,
        domain_name: str = "Platform Service",
        domain_version: str = "1",
        verifying_contract: str = "0x0000000000000000000000000000000000000000",
        signed_headers: tuple[str, ...] = DEFAULT_SIGNED_HEADERS,
    ) -> dict[str, Any]:
        split = urlsplit(url)
        request_headers = {
            "content-type": content_type,
            "x-request-id": request_id,
        }

        return {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
                "APIRequest": [
                    {"name": "method", "type": "string"},
                    {"name": "host", "type": "string"},
                    {"name": "path", "type": "string"},
                    {"name": "queryHash", "type": "bytes32"},
                    {"name": "headersHash", "type": "bytes32"},
                    {"name": "bodyHash", "type": "bytes32"},
                    {"name": "nonce", "type": "uint256"},
                    {"name": "issuedAt", "type": "uint256"},
                    {"name": "expiresAt", "type": "uint256"},
                ],
            },
            "primaryType": "APIRequest",
            "domain": {
                "name": domain_name,
                "version": domain_version,
                "chainId": chain_id,
                "verifyingContract": verifying_contract,
            },
            "message": {
                "method": method.upper(),
                "host": split.netloc,
                "path": split.path or "/",
                "queryHash": _hash_query(url),
                "headersHash": _hash_headers(request_headers, signed_headers),
                "bodyHash": _hash_body(body, content_type),
                "nonce": nonce,
                "issuedAt": now,
                "expiresAt": now + 60,
            },
        }

    def build_auth_headers(
        self,
        method: str,
        url: str,
        body: Any = None,
        *,
        content_type: str = "application/json",
        request_id: str | None = None,
        chain_id: int = 1,
    ) -> dict[str, str]:
        now = int(time.time())
        nonce = secrets.randbits(52)
        request_id = request_id or f"req-{now}"
        typed_data = self.build_typed_data(
            method=method,
            url=url,
            body=body,
            content_type=content_type,
            request_id=request_id,
            now=now,
            nonce=nonce,
            chain_id=chain_id,
        )
        signature = self.sign_typed_data(typed_data)
        message = typed_data["message"]
        return {
            "Content-Type": content_type,
            "X-Request-ID": request_id,
            "X-Signer": self.get_address(),
            "X-Signature": signature,
            "X-Nonce": str(message["nonce"]),
            "X-Issued-At": str(message["issuedAt"]),
            "X-Expires-At": str(message["expiresAt"]),
            "X-Chain-Id": str(chain_id),
            "X-Signed-Headers": ",".join(DEFAULT_SIGNED_HEADERS),
        }
