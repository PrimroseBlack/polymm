"""`polyrec clob-creds`: obtain the CLOB API credentials the Chainlink feed needs.

By default this creates a brand-new, empty wallet just for the recorder, so the credentials on
the server are tied to nothing of value. The wallet's private key is used once, in memory, to
sign Polymarket's ClobAuth message, and is then discarded.
"""
from __future__ import annotations

import getpass
import os
import time
from pathlib import Path

import httpx

from .config import CRED_VARS, Config

AUTH_MESSAGE = "This message attests that I control the given wallet"


def clob_auth_typed_data(address: str, timestamp: int, nonce: int = 0) -> dict:
    return {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
            ],
            "ClobAuth": [
                {"name": "address", "type": "address"},
                {"name": "timestamp", "type": "string"},
                {"name": "nonce", "type": "uint256"},
                {"name": "message", "type": "string"},
            ],
        },
        "primaryType": "ClobAuth",
        "domain": {"name": "ClobAuthDomain", "version": "1", "chainId": 137},
        "message": {"address": address, "timestamp": str(timestamp), "nonce": nonce, "message": AUTH_MESSAGE},
    }


def l1_headers(account, timestamp: int | None = None, nonce: int = 0) -> dict:
    timestamp = int(time.time()) if timestamp is None else timestamp
    signed = account.sign_typed_data(full_message=clob_auth_typed_data(account.address, timestamp, nonce))
    sig = signed.signature.hex()
    return {
        "POLY_ADDRESS": account.address,
        "POLY_SIGNATURE": sig if sig.startswith("0x") else "0x" + sig,
        "POLY_TIMESTAMP": str(timestamp),
        "POLY_NONCE": str(nonce),
    }


def create_or_derive(client: httpx.Client, clob_url: str, account) -> dict:
    """Create API credentials for the wallet, or fetch the existing ones."""
    r = client.post(f"{clob_url}/auth/api-key", headers=l1_headers(account))
    if r.status_code != 200 or "apiKey" not in r.text:
        r = client.get(f"{clob_url}/auth/derive-api-key", headers=l1_headers(account))
    if r.status_code != 200:
        raise RuntimeError(f"Polymarket refused to issue credentials: HTTP {r.status_code} {r.text[:200]}")
    body = r.json()
    return {CRED_VARS[0]: body["apiKey"], CRED_VARS[1]: body["secret"], CRED_VARS[2]: body["passphrase"]}


def write_env(path: Path, values: dict, note: str) -> None:
    """Write KEY=VALUE lines readable only by the owner."""
    tmp = path.with_name(path.name + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(f"# Polymarket CLOB API credentials for polyrec ({note}). Keep this file private.\n")
        for k, v in values.items():
            f.write(f"{k}={v}\n")
    tmp.replace(path)


def clob_creds_cmd(cfg: Config, out: str, key_env: str | None, prompt: bool, force: bool) -> int:
    try:
        from eth_account import Account
    except ImportError:
        print("this command needs the eth-account package:  pip install 'polyrec[creds]'")
        return 1
    path = Path(out)
    if path.exists() and not force:
        print(f"{path} already exists; pass --force to replace it")
        return 1
    if key_env:
        key = os.environ.get(key_env)
        if not key:
            print(f"environment variable {key_env} is not set")
            return 1
        account, note = Account.from_key(key), "existing wallet"
    elif prompt:
        account, note = Account.from_key(getpass.getpass("Wallet private key (input hidden): ").strip()), "existing wallet"
    else:
        account, note = Account.create(), "throwaway wallet created for the recorder"
    with httpx.Client(timeout=20, headers={"User-Agent": "polyrec"}) as client:
        values = create_or_derive(client, cfg.clob_url, account)
    write_env(path, values, f"{note} {account.address}")
    print(f"wrote {path} (mode 600) for wallet {account.address} ({note})")
    print("install it on the recorder host as /etc/polyrec.env, then run `polyrec smoke`")
    return 0
