from __future__ import annotations

import json

from signer import WalletSigner


def test_build_typed_data_is_deterministic() -> None:
    signer = WalletSigner(wallet_bin="awp-wallet", session_token="token")

    typed_data = signer.build_typed_data(
        method="POST",
        url="http://101.47.73.95/api/mining/v1/heartbeat",
        body={"client": "debug-client", "ip_address": ""},
        content_type="application/json",
        request_id="req-123",
        now=1700000000,
        nonce=123456789,
    )

    assert typed_data["primaryType"] == "APIRequest"
    assert typed_data["domain"] == {
        "name": "Platform Service",
        "version": "1",
        "chainId": 1,
        "verifyingContract": "0x0000000000000000000000000000000000000000",
    }
    assert typed_data["message"]["method"] == "POST"
    assert typed_data["message"]["host"] == "101.47.73.95"
    assert typed_data["message"]["path"] == "/api/mining/v1/heartbeat"
    assert typed_data["message"]["nonce"] == 123456789
    assert typed_data["message"]["issuedAt"] == 1700000000
    assert typed_data["message"]["expiresAt"] == 1700000060
    assert typed_data["message"]["bodyHash"].startswith("0x")
    assert typed_data["message"]["headersHash"].startswith("0x")


def test_build_auth_headers_uses_typed_data_fields(monkeypatch) -> None:
    signer = WalletSigner(wallet_bin="awp-wallet", session_token="token")
    monkeypatch.setattr(signer, "get_address", lambda: "0x2222222222222222222222222222222222222222")
    monkeypatch.setattr(signer, "sign_typed_data", lambda payload: "0xsigned")
    monkeypatch.setattr("signer.time.time", lambda: 1700000100)
    monkeypatch.setattr("signer.secrets.randbits", lambda bits: 424242)

    headers = signer.build_auth_headers(
        "POST",
        "http://101.47.73.95/api/mining/v1/miners/preflight",
        {"dataset_id": "dataset-1", "epoch_id": "epoch-1"},
    )

    assert headers == {
        "Content-Type": "application/json",
        "X-Request-ID": "req-1700000100",
        "X-Signer": "0x2222222222222222222222222222222222222222",
        "X-Signature": "0xsigned",
        "X-Nonce": "424242",
        "X-Issued-At": "1700000100",
        "X-Expires-At": "1700000160",
        "X-Chain-Id": "1",
        "X-Signed-Headers": "content-type,x-request-id",
    }


def test_get_address_accepts_eoa_address_shape(monkeypatch) -> None:
    signer = WalletSigner(wallet_bin="awp-wallet", session_token="token")
    monkeypatch.setattr(signer, "_run", lambda *args: {"eoaAddress": "0x3333333333333333333333333333333333333333"})

    assert signer.get_address() == "0x3333333333333333333333333333333333333333"


def test_build_typed_data_uses_empty_hashes_for_missing_query_and_body() -> None:
    signer = WalletSigner(wallet_bin="awp-wallet", session_token="token")

    typed_data = signer.build_typed_data(
        method="GET",
        url="http://101.47.73.95/api/core/v1/datasets",
        body=None,
        content_type="application/json",
        request_id="req-abc",
        now=1700000200,
        nonce=999,
    )

    empty_hash = "0x" + ("0" * 64)
    assert typed_data["message"]["queryHash"] == empty_hash
    assert typed_data["message"]["bodyHash"] == empty_hash
