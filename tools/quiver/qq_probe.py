import os
import requests
import json

BASES = [os.getenv("QQ_BASE") or "https://api.quiverquant.com/beta",
         "https://api.quiverquant.com/v1"]
TOK  = os.getenv("QQ_TOKEN") or os.getenv("QUIVER_API_TOKEN")
HEAD = {"Authorization": f"Bearer {TOK}", "Accept": "application/json"}

CANDS = [
  "/recent/govcontracts",
  "/recent/govcontractsall",
  "/live/govcontracts",
  "/live/govcontractsall",
  "/recent/governmentcontracts",
  "/recent/governmentcontractsall",
  "/live/governmentcontracts",
  "/live/governmentcontractsall",
]

def keys_of(x):
  if isinstance(x, list) and x:
    x = x[0]
  return sorted(list(x.keys())) if isinstance(x, dict) else []

def find_govcontracts_recent():
  print("Scanning for govcontracts recent/live across /beta and /v1 …")
  for base in BASES:
    for p in CANDS:
      url = base + p
      try:
        r = requests.get(url, headers=HEAD, timeout=20)
        print(f"{url:70s} -> {r.status_code}")
        if r.ok:
          try:
            data = r.json()
            print("  sample keys:", keys_of(data))
            return base, p, keys_of(data)
          except Exception:
            pass
      except Exception as e:
        print("  err:", e)
  return None, None, None

if __name__ == "__main__":
  if not TOK:
    print("Set QUIVER_API_TOKEN/QQ_TOKEN")
    raise SystemExit(1)
  b, p, k = find_govcontracts_recent()
  print("=> chosen:", b, p, k)
