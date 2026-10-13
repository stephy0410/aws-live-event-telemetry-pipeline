"""API Gateway (HTTP API) -> Lambda: the live operations dashboard and its read-only JSON API.

  GET /                                    the dashboard page (static/index.html)
  GET /api/overview                        platform throughput, planners, latest alerts
  GET /api/planners/{planner_id}/events    one planner's events (the tenant view)
  GET /api/events/{event_id}               zones, last 30 minutes of aggregates, alerts

Every response is built from Query/GetItem calls on known keys (no table scans).
"""

import json
import logging
import os
import pathlib
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import boto3

import common as c

log = logging.getLogger()
log.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

WINDOW_MINUTES = 30
ACTIVE_SECONDS = 120  # an event is "live" if a sensor reported in the last 2 minutes
ID_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{1,64}$")
INTERNAL_ATTRIBUTES = {"pk", "sk", "gsi1pk", "gsi1sk", "ttl"}

# Alert thresholds come from the same Terraform variables the processor uses, so the
# page colors a zone exactly when the pipeline would alert on it.
DASHBOARD_CONFIG = {
    "grafana_url": os.environ.get("GRAFANA_URL", ""),
    "occupancy_warning_ratio": float(os.environ.get("OCCUPANCY_WARNING_RATIO", "0.9")),
    "cold_room_max_c": float(os.environ.get("COLD_ROOM_MAX_C", "5")),
    "comfort_max_c": float(os.environ.get("COMFORT_MAX_C", "28")),
    "noise_warning_db": float(os.environ.get("NOISE_WARNING_DB", "95")),
    "noise_critical_db": float(os.environ.get("NOISE_CRITICAL_DB", "105")),
}
PAGE = (pathlib.Path(__file__).parent / "static" / "index.html").read_text(encoding="utf-8")
PAGE = PAGE.replace("{{DASHBOARD_CONFIG}}", json.dumps(DASHBOARD_CONFIG).replace("</", "<\\/"))

ddb = boto3.client("dynamodb")
_pool = ThreadPoolExecutor(max_workers=8)


def handler(event, context):
    route = event.get("routeKey", "")
    params = event.get("pathParameters") or {}
    try:
        if route == "GET /":
            return _response(200, PAGE, "text/html; charset=utf-8")
        if route == "GET /api/overview":
            return _json(200, overview())
        if route == "GET /api/planners/{planner_id}/events":
            planner_id = params.get("planner_id", "")
            if not ID_PATTERN.match(planner_id):
                return _json(400, {"error": "invalid planner_id"})
            return _json(200, {"planner_id": planner_id, "events": planner_events(planner_id)})
        if route == "GET /api/events/{event_id}":
            event_id = params.get("event_id", "")
            if not ID_PATTERN.match(event_id):
                return _json(400, {"error": "invalid event_id"})
            detail = event_detail(event_id)
            return _json(200, detail) if detail else _json(404, {"error": "event not found"})
        return _json(404, {"error": "not found"})
    except Exception:
        log.exception("request failed: %s", route)
        return _json(500, {"error": "internal error"})


# --- views ---

def overview():
    now = c.now_utc()
    first_minute = c.minute_bucket(now - timedelta(minutes=WINDOW_MINUTES - 1))

    planners = _query("pk = :pk", {":pk": "PLANNERS"})
    events_by_planner = dict(zip(
        [p["planner_id"] for p in planners],
        _pool.map(planner_events, [p["planner_id"] for p in planners]),
    ))
    for p in planners:
        events = events_by_planner[p["planner_id"]]
        p["events"] = len(events)
        p["live_events"] = sum(1 for e in events if e["live"])
        p["checked_in"] = sum(e.get("checked_in", 0) for e in events)
        p["guests_expected"] = sum(e.get("guests_expected", 0) for e in events)
        p["alerts_total"] = sum(e.get("alerts_total", 0) for e in events)
    planners.sort(key=lambda p: p.get("planner_name", p["planner_id"]))

    stats = _query(
        "pk = :pk AND sk BETWEEN :from AND :to",
        {":pk": "STATS", ":from": f"MINUTE#{first_minute}", ":to": f"MINUTE#{c.minute_bucket(now)}~"},
    )
    by_minute = {s["minute"]: s for s in stats}
    throughput = []
    for i in range(WINDOW_MINUTES - 1, -1, -1):
        minute = c.minute_bucket(now - timedelta(minutes=i))
        s = by_minute.get(minute, {})
        throughput.append({
            "minute": minute,
            "records": s.get("records", 0),
            "invalid": s.get("invalid", 0),
            "alerts": s.get("alerts", 0),
        })

    return {
        "generated_at": c.iso(now),
        "window_minutes": WINDOW_MINUTES,
        "throughput": throughput,
        "planners": planners,
        "alerts": recent_alerts(now, limit=25),
    }


def planner_events(planner_id):
    now = c.now_utc()
    events = _query(
        "gsi1pk = :pk AND begins_with(gsi1sk, :prefix)",
        {":pk": c.planner_gsi_pk(planner_id), ":prefix": "EVENT#"},
        IndexName="gsi1",
    )
    for e in events:
        e["live"] = _is_live(e.get("last_seen"), now)
    events.sort(key=lambda e: (not e["live"], e.get("event_name", e["event_id"])))
    return events


def event_detail(event_id):
    pk = c.event_pk(event_id)
    response = ddb.get_item(TableName=c.TABLE_NAME, Key=c.key(pk, "META"))
    if "Item" not in response:
        return None
    meta = _clean(c.unmarshal_item(response["Item"]))
    now = c.now_utc()
    meta["live"] = _is_live(meta.get("last_seen"), now)

    zones = _query("pk = :pk AND begins_with(sk, :prefix)", {":pk": pk, ":prefix": "STATE#"})
    order = {z["zone"]: i for i, z in enumerate(meta.get("zones") or [])}
    zones.sort(key=lambda z: order.get(z["zone"], len(order)))

    first_minute = c.minute_bucket(now - timedelta(minutes=WINDOW_MINUTES - 1))
    aggregates = _query(
        "pk = :pk AND sk BETWEEN :from AND :to",
        {":pk": pk, ":from": f"AGG#{first_minute}", ":to": f"AGG#{c.minute_bucket(now)}~"},
    )
    series = []
    for a in aggregates:
        point = {"minute": a["minute"], "zone": a["zone"]}
        for metric in ("occupancy", "temperature_c", "noise_db"):
            count = a.get(f"{metric}_count", 0)
            if count:
                point[metric] = round(float(a[f"{metric}_sum"]) / float(count), 1)
        if a.get("checkin_sum"):
            point["checkins"] = int(a["checkin_sum"])
        series.append(point)

    alerts = _query(
        "pk = :pk AND begins_with(sk, :prefix)",
        {":pk": pk, ":prefix": "ALERT#"},
        ScanIndexForward=False, Limit=20, max_items=20,
    )
    return {"generated_at": c.iso(now), "event": meta, "zones": zones, "series": series, "alerts": alerts}


def recent_alerts(now, limit):
    """Alerts are indexed per UTC day (gsi1pk = ALERTS#<day>): today first, then yesterday."""
    alerts = []
    for days_back in (0, 1):
        day = c.day_bucket(now - timedelta(days=days_back))
        alerts += _query(
            "gsi1pk = :pk", {":pk": c.alerts_gsi_pk(day)},
            IndexName="gsi1", ScanIndexForward=False, Limit=limit, max_items=limit - len(alerts),
        )
        if len(alerts) >= limit:
            break
    return alerts


# --- helpers ---

def _query(condition, values, max_items=None, **kwargs):
    items, start_key = [], None
    while True:
        request = {
            "TableName": c.TABLE_NAME,
            "KeyConditionExpression": condition,
            "ExpressionAttributeValues": {k: c.marshal(v) for k, v in values.items()},
            **kwargs,
        }
        if start_key:
            request["ExclusiveStartKey"] = start_key
        response = ddb.query(**request)
        items += [_clean(c.unmarshal_item(i)) for i in response.get("Items", [])]
        start_key = response.get("LastEvaluatedKey")
        if not start_key or (max_items and len(items) >= max_items):
            return items[:max_items] if max_items else items


def _clean(item):
    return {k: v for k, v in item.items() if k not in INTERNAL_ATTRIBUTES}


def _is_live(last_seen, now):
    if not last_seen:
        return False
    return (now - c.parse_ts(last_seen)).total_seconds() <= ACTIVE_SECONDS


def _json(status, body):
    return _response(status, c.dumps(body), "application/json")


def _response(status, body, content_type):
    return {
        "statusCode": status,
        "headers": {"content-type": content_type, "cache-control": "no-store"},
        "body": body,
    }
