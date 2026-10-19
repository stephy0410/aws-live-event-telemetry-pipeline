"""Load test for the dashboard API: the same request mix without and with the read cache.

  python3 scripts/api_load_test.py --url "$(terraform output -raw dashboard_url)" --requests 600 --concurrency 10

1. Discovers the planner and event ids from /api/overview.
2. "no cache": every request carries `Cache-Control: no-cache`, so each one reads DynamoDB.
3. "cache": a short warm-up loads the cache, then the same mix runs as a normal client.
4. Prints both runs side by side: throughput, client latency, time inside the Lambda
   (server-timing), cache hit ratio and DynamoDB calls (x-dynamodb-calls).

Each Lambda execution environment has its own cache, so with N concurrent clients up to N
environments each miss once per key and TTL: the hit ratio is measured, not assumed.
Standard library only; it runs against the deployed API or `make local-demo`.
"""

import argparse
import json
import random
import statistics
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor


def get(base_url, path, no_cache, timeout=15):
    """-> (status, client ms, server ms or None, x-cache, DynamoDB calls, body bytes)"""
    request = urllib.request.Request(base_url + path, headers={"Cache-Control": "no-cache"} if no_cache else {})
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body, status, headers = response.read(), response.status, response.headers
    except urllib.error.HTTPError as e:
        body, status, headers = e.read(), e.code, e.headers
    except (urllib.error.URLError, TimeoutError):
        return 0, (time.perf_counter() - started) * 1000, None, "", 0, b""
    client_ms = (time.perf_counter() - started) * 1000
    server_ms = None
    for part in (headers.get("server-timing") or "").split(";"):
        if part.strip().startswith("dur="):
            server_ms = float(part.strip()[4:])
    return status, client_ms, server_ms, headers.get("x-cache", ""), int(headers.get("x-dynamodb-calls") or 0), body


def discover_paths(base_url):
    status, *_, body = get(base_url, "/api/overview", no_cache=True)
    if status != 200:
        sys.exit(f"GET /api/overview returned {status}: is the URL right and is the simulator sending data?")
    planners = [p["planner_id"] for p in json.loads(body)["planners"]]
    events = []
    for planner_id in planners:
        status, *_, body = get(base_url, f"/api/planners/{planner_id}/events", no_cache=True)
        if status == 200:
            events += [e["event_id"] for e in json.loads(body)["events"]]
    if not planners or not events:
        sys.exit("No planners or events yet: run `make simulate` for a minute first.")
    return planners, events


def request_mix(planners, events, n, seed):
    """Like real traffic: half overviews (the landing page), the rest planner and event views."""
    rng = random.Random(seed)
    paths = []
    for _ in range(n):
        roll = rng.random()
        if roll < 0.5:
            paths.append("/api/overview")
        elif roll < 0.75:
            paths.append(f"/api/planners/{rng.choice(planners)}/events")
        else:
            paths.append(f"/api/events/{rng.choice(events)}")
    return paths


def run(base_url, paths, concurrency, no_cache):
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        results = list(pool.map(lambda p: get(base_url, p, no_cache), paths))
    return results, time.perf_counter() - started


def percentile(values, p):
    if not values:
        return float("nan")
    values = sorted(values)
    return values[min(len(values) - 1, round(p / 100 * (len(values) - 1)))]


def summarize(results, elapsed):
    ok = [r for r in results if r[0] == 200]
    client = [r[1] for r in ok]
    server = [r[2] for r in ok if r[2] is not None]
    hits = sum(1 for r in ok if r[3] == "HIT")
    return {
        "requests": len(results),
        "ok": len(ok),
        "throttled": sum(1 for r in results if r[0] == 429),
        "errors": sum(1 for r in results if r[0] not in (200, 429)),
        "rps": len(results) / elapsed,
        "p50": percentile(client, 50),
        "p95": percentile(client, 95),
        "p99": percentile(client, 99),
        "mean": statistics.fmean(client) if client else float("nan"),
        "server_p50": percentile(server, 50),
        "server_p95": percentile(server, 95),
        "hit_ratio": 100 * hits / len(ok) if ok else 0.0,
        "ddb_calls": sum(r[4] for r in ok),
    }


def print_report(cold, warm):
    rows = [
        ("Requests sent", "requests", "{:,.0f}"),
        ("Successful (200)", "ok", "{:,.0f}"),
        ("Throttled (429)", "throttled", "{:,.0f}"),
        ("Errors", "errors", "{:,.0f}"),
        ("Throughput (req/s)", "rps", "{:,.1f}"),
        ("Client latency p50 (ms)", "p50", "{:,.1f}"),
        ("Client latency p95 (ms)", "p95", "{:,.1f}"),
        ("Client latency p99 (ms)", "p99", "{:,.1f}"),
        ("Client latency mean (ms)", "mean", "{:,.1f}"),
        ("Time in Lambda p50 (ms)", "server_p50", "{:,.1f}"),
        ("Time in Lambda p95 (ms)", "server_p95", "{:,.1f}"),
        ("Cache hit ratio (%)", "hit_ratio", "{:,.1f}"),
        ("DynamoDB calls", "ddb_calls", "{:,.0f}"),
    ]
    better_when_lower = {"p50", "p95", "p99", "mean", "server_p50", "server_p95", "ddb_calls"}
    print()
    print(f"{'':28}{'no cache':>14}{'cache':>14}{'change':>12}")
    print("-" * 68)
    for label, key, fmt in rows:
        a, b = cold[key], warm[key]
        change = ""
        if key in better_when_lower and a:
            change = f"{(b - a) / a * 100:+.0f}%"
        elif key == "rps" and a:
            change = f"x{b / a:.2f}"
        print(f"{label:28}{fmt.format(a):>14}{fmt.format(b):>14}{change:>12}")
    print()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", required=True, help="API base URL (terraform output -raw dashboard_url)")
    parser.add_argument("--requests", type=int, default=600, help="requests per run")
    parser.add_argument("--concurrency", type=int, default=10, help="parallel clients")
    parser.add_argument("--warmup", type=int, default=100, help="unmeasured requests that load the cache")
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()
    base_url = args.url.rstrip("/")

    planners, events = discover_paths(base_url)
    paths = request_mix(planners, events, args.requests, args.seed)
    print(f"API: {base_url}")
    print(f"{len(planners)} planners, {len(events)} events; {args.requests} requests per run, {args.concurrency} parallel clients")

    print("Run 1/2: no cache (Cache-Control: no-cache, every request reads DynamoDB)...")
    cold = summarize(*run(base_url, paths, args.concurrency, no_cache=True))

    print(f"Run 2/2: cache loaded ({args.warmup} warm-up requests, then the same mix)...")
    run(base_url, request_mix(planners, events, args.warmup, args.seed + 1), args.concurrency, no_cache=False)
    warm = summarize(*run(base_url, paths, args.concurrency, no_cache=False))

    print_report(cold, warm)


if __name__ == "__main__":
    main()
