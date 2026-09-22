"""Small interactive client for the JSON line protocol."""
from __future__ import annotations

import argparse
import json
import socket


def request(address: str, payload: dict) -> dict:
    host, port = address.rsplit(":", 1)
    with socket.create_connection((host, int(port)), timeout=2) as conn:
        conn.sendall(json.dumps(payload).encode() + b"\n")
        return json.loads(conn.makefile("rb").readline())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--addr", required=True)
    args = parser.parse_args()
    print("Commands: SET key value | GET key | DELETE key | STATUS | QUIT")
    while True:
        try: raw = input("dkv> ").strip()
        except EOFError: return
        if raw.upper() in {"QUIT", "EXIT"}: return
        parts = raw.split(maxsplit=2)
        if not parts: continue
        op = parts[0].lower()
        payload = {"op": op}
        if op in {"set", "get", "delete"} and len(parts) >= 2: payload["key"] = parts[1]
        if op == "set" and len(parts) == 3: payload["value"] = parts[2]
        print(json.dumps(request(args.addr, payload), indent=2))


if __name__ == "__main__":
    main()
