"""Kinesis -> Lambda stream processor (the real-time path).

For each batch of records from one shard:
  1. decode and validate every record (malformed ones are counted and skipped, never retried)
  2. archive_mode = lambda: write the raw batch to the S3 data lake (idempotent key)
  3. pre-aggregate in memory, so DynamoDB gets one write per (event, zone, minute)
     instead of one per reading, and write live state, aggregates, metadata and
     platform stats in parallel
  4. evaluate alert rules; alerts that pass the per-zone cooldown go to the SNS topic
  5. emit custom CloudWatch metrics with Embedded Metric Format (a log line, no API call)

Delivery is at-least-once: a retried batch adds its counters again. Live state is
idempotent and alerts are deduplicated, but per-minute counters can over-count after
a failure, which is why exact history is answered from the raw data lake (Athena).
"""

import base64
import gzip
import json
import logging
import os
import time
import unicodedata
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

import common as c

log = logging.getLogger()
log.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

TOPIC_ARN = os.environ.get("ALERTS_TOPIC_ARN", "")
ARCHIVE_BUCKET = os.environ.get("ARCHIVE_BUCKET", "")
AGGREGATE_TTL_DAYS = int(os.environ.get("AGGREGATE_TTL_DAYS", "7"))

OCCUPANCY_WARNING_RATIO = float(os.environ.get("OCCUPANCY_WARNING_RATIO", "0.9"))
COLD_ROOM_MAX_C = float(os.environ.get("COLD_ROOM_MAX_C", "5"))
COMFORT_MAX_C = float(os.environ.get("COMFORT_MAX_C", "28"))
NOISE_WARNING_DB = float(os.environ.get("NOISE_WARNING_DB", "95"))
NOISE_CRITICAL_DB = float(os.environ.get("NOISE_CRITICAL_DB", "105"))
ALERT_COOLDOWN_SECONDS = int(os.environ.get("ALERT_COOLDOWN_SECONDS", "300"))

STATE_METRICS = ("occupancy", "temperature_c", "noise_db")  # check-ins are counted, not "current"
SEVERITY_RANK = {"warning": 1, "critical": 2}
ALERT_ID_NAMESPACE = uuid.UUID("6f1c9a52-9d1e-4f43-9a52-3c0d1b7e8a10")

# Clients and the pool live outside the handler, so warm invocations reuse their connections.
_aws_config = Config(retries={"mode": "standard", "max_attempts": 5}, max_pool_connections=32)
ddb = boto3.client("dynamodb", config=_aws_config)
sns = boto3.client("sns", config=_aws_config)
s3 = boto3.client("s3", config=_aws_config)
_pool = ThreadPoolExecutor(max_workers=16)


def handler(event, context):
    started = time.time()
    records = event.get("Records", [])

    readings, registrations = [], []
    per_minute = defaultdict(lambda: {"records": 0, "invalid": 0})
    for record in records:
        arrived = datetime.fromtimestamp(record["kinesis"].get("approximateArrivalTimestamp", started), timezone.utc)
        bucket = per_minute[c.minute_bucket(arrived)]
        bucket["records"] += 1
        try:
            msg = c.decode(record)
        except c.InvalidRecord as e:
            bucket["invalid"] += 1
            log.warning("skipping invalid record %s: %s", record["kinesis"].get("sequenceNumber"), e)
            continue
        (readings if msg["kind"] == "sensor.reading" else registrations).append(msg)

    if ARCHIVE_BUCKET and records:
        archive(records)

    writes = (
        registration_writes(registrations)
        + event_writes(readings)
        + zone_state_writes(readings)
        + aggregate_writes(readings)
        + stats_writes(per_minute)
    )
    # Any write that still fails after the SDK's retries raises here, so Lambda retries
    # the batch (and bisects it) instead of silently dropping data.
    list(_pool.map(lambda req: ddb.update_item(**req), writes))

    alerts = detect_alerts(readings)
    published = publish_alerts(alerts)

    done = time.time()
    latencies_ms = [(done - r["_dt"].timestamp()) * 1000 for r in readings]
    invalid = sum(b["invalid"] for b in per_minute.values())
    emit_metrics(len(readings) + len(registrations), invalid, len(writes), published, latencies_ms)

    summary = {
        "records": len(records),
        "readings": len(readings),
        "registrations": len(registrations),
        "invalid": invalid,
        "dynamodb_writes": len(writes),
        "alerts_detected": len(alerts),
        "alerts_published": published,
        "duration_ms": round((done - started) * 1000),
    }
    log.info("batch processed %s", json.dumps(summary))
    return summary


# --- DynamoDB writes (one UpdateItem per item touched in this batch) ---

def registration_writes(registrations):
    latest = {m["event_id"]: m for m in sorted(registrations, key=lambda m: m["_dt"])}
    requests, planners = [], {}
    for event_id, m in latest.items():
        requests.append(c.update_request(c.event_pk(event_id), "META", set_values={
            "event_id": event_id,
            "planner_id": m["planner_id"],
            "planner_name": m["planner_name"],
            "event_name": m["event_name"],
            "event_type": m["event_type"],
            "venue": m.get("venue"),
            "city": m.get("city"),
            "guests_expected": m.get("guests_expected"),
            "starts_at": m.get("starts_at"),
            "zones": m.get("zones"),
            "registered_at": m["ts"],
            "gsi1pk": c.planner_gsi_pk(m["planner_id"]),
            "gsi1sk": c.event_pk(event_id),
        }))
        planners[m["planner_id"]] = m
    for planner_id, m in planners.items():
        requests.append(c.update_request("PLANNERS", f"PLANNER#{planner_id}", set_values={
            "planner_id": planner_id,
            "planner_name": m["planner_name"],
            "last_seen": m["ts"],
        }))
    return requests


def event_writes(readings):
    per_event = {}
    for r in readings:
        e = per_event.setdefault(r["event_id"], {"planner_id": r["planner_id"], "readings": 0, "checkins": 0, "last": r["_dt"]})
        e["readings"] += 1
        if r["metric"] == "checkin":
            e["checkins"] += int(r["value"])
        e["last"] = max(e["last"], r["_dt"])

    return [
        c.update_request(
            c.event_pk(event_id), "META",
            set_values={
                "event_id": event_id,
                "planner_id": e["planner_id"],
                "last_seen": c.iso(e["last"]),
                "gsi1pk": c.planner_gsi_pk(e["planner_id"]),
                "gsi1sk": c.event_pk(event_id),
            },
            add_values={"readings": e["readings"], "checked_in": e["checkins"]},
        )
        for event_id, e in per_event.items()
    ]


def zone_state_writes(readings):
    """Latest value of each metric per zone. Kinesis keeps records of one partition key
    (event_id) in order, so a later batch never carries older readings for the same event."""
    latest = defaultdict(dict)
    for r in readings:
        if r["metric"] in STATE_METRICS:
            current = latest[(r["event_id"], r["zone"])].get(r["metric"])
            if current is None or r["_dt"] >= current["_dt"]:
                latest[(r["event_id"], r["zone"])][r["metric"]] = r

    requests = []
    for (event_id, zone), metrics in latest.items():
        values = {"zone": zone}
        for metric, r in metrics.items():
            values[metric] = r["value"]
            values[f"{metric}_at"] = r["ts"]
            if metric == "occupancy":
                values["capacity"] = r["capacity"]
        requests.append(c.update_request(c.event_pk(event_id), f"STATE#{zone}", set_values=values))
    return requests


def aggregate_writes(readings):
    """Per-minute count and sum of every metric per zone (average = sum / count)."""
    buckets = defaultdict(lambda: defaultdict(int))
    for r in readings:
        counters = buckets[(r["event_id"], c.minute_bucket(r["_dt"]), r["zone"])]
        counters[f"{r['metric']}_count"] += 1
        counters[f"{r['metric']}_sum"] += r["value"]

    ttl = c.ttl_after(AGGREGATE_TTL_DAYS)
    return [
        c.update_request(
            c.event_pk(event_id), f"AGG#{minute}#{zone}",
            set_values={"zone": zone, "minute": minute, "ttl": ttl},
            add_values=dict(counters),
        )
        for (event_id, minute, zone), counters in buckets.items()
    ]


def stats_writes(per_minute):
    """Platform-wide throughput, by the minute records reached Kinesis."""
    ttl = c.ttl_after(AGGREGATE_TTL_DAYS)
    return [
        c.update_request(
            "STATS", f"MINUTE#{minute}",
            set_values={"minute": minute, "ttl": ttl},
            add_values={k: v for k, v in counts.items() if v},
        )
        for minute, counts in per_minute.items()
    ]


# --- alerts ---

def evaluate(r):
    """Alert rules for one reading -> (alert_type, severity, threshold, message) or None."""
    metric, value, zone = r["metric"], r["value"], r["zone"]
    if metric == "occupancy":
        capacity = r["capacity"]
        ratio = value / capacity
        if ratio >= 1.0:
            return "capacity", "critical", capacity, f"{zone} is over capacity: {value:.0f}/{capacity} people ({ratio:.0%})"
        if ratio >= OCCUPANCY_WARNING_RATIO:
            return "capacity", "warning", round(capacity * OCCUPANCY_WARNING_RATIO), f"{zone} is near capacity: {value:.0f}/{capacity} people ({ratio:.0%})"
    elif metric == "temperature_c":
        if zone == "cold-room":
            if value > COLD_ROOM_MAX_C:
                return "cold_chain", "critical", COLD_ROOM_MAX_C, f"Cold room at {value:.1f} °C, above the {COLD_ROOM_MAX_C:g} °C food-safety limit"
        elif value > COMFORT_MAX_C:
            return "comfort", "warning", COMFORT_MAX_C, f"{zone} is too hot for guests: {value:.1f} °C"
    elif metric == "noise_db":
        if value >= NOISE_CRITICAL_DB:
            return "noise", "critical", NOISE_CRITICAL_DB, f"{zone} at {value:.0f} dB, over the venue limit of {NOISE_CRITICAL_DB:g} dB"
        if value >= NOISE_WARNING_DB:
            return "noise", "warning", NOISE_WARNING_DB, f"{zone} is loud: {value:.0f} dB"
    return None


def detect_alerts(readings):
    """At most one alert per (event, zone, type) per batch: the most severe, then most recent, breach."""
    found = {}
    for r in readings:
        rule = evaluate(r)
        if rule is None:
            continue
        alert_type, severity, threshold, message = rule
        slot = (r["event_id"], r["zone"], alert_type)
        current = found.get(slot)
        if current and (SEVERITY_RANK[current["severity"]], current["_dt"]) > (SEVERITY_RANK[severity], r["_dt"]):
            continue
        found[slot] = {
            # Derived from the reading, so a retried batch produces the same id and the
            # alert worker's conditional write drops the duplicate.
            "alert_id": str(uuid.uuid5(ALERT_ID_NAMESPACE, f"{r['reading_id']}|{alert_type}|{severity}")),
            "event_id": r["event_id"],
            "planner_id": r["planner_id"],
            "zone": r["zone"],
            "sensor_id": r["sensor_id"],
            "reading_id": r["reading_id"],
            "alert_type": alert_type,
            "severity": severity,
            "metric": r["metric"],
            "value": r["value"],
            "threshold": threshold,
            "message": message,
            "reading_ts": r["ts"],
            "detected_at": c.iso(c.now_utc()),
            "_dt": r["_dt"],
        }
    return list(found.values())


def publish_alerts(alerts):
    if not alerts or not TOPIC_ARN:
        return 0
    now = int(time.time())
    claimed = [a for a, ok in zip(alerts, _pool.map(lambda a: claim_cooldown(a, now), alerts)) if ok]

    published = 0
    for i in range(0, len(claimed), 10):  # PublishBatch takes up to 10 messages
        chunk = claimed[i:i + 10]
        entries = [sns_entry(str(j), alert) for j, alert in enumerate(chunk)]
        try:
            response = sns.publish_batch(TopicArn=TOPIC_ARN, PublishBatchRequestEntries=entries)
            failed = {f["Id"] for f in response.get("Failed", [])}
        except ClientError:
            log.exception("SNS publish_batch failed")
            failed = {e["Id"] for e in entries}
        for j, alert in enumerate(chunk):
            if str(j) in failed:
                # Give the cooldown back: the condition is still true, so the next
                # reading from that sensor will raise the alert again.
                release_cooldown(alert)
            else:
                published += 1
    return published


def _cooldown_key(alert):
    return c.key(c.event_pk(alert["event_id"]), f"COOLDOWN#{alert['zone']}#{alert['alert_type']}#{alert['severity']}")


def claim_cooldown(alert, now):
    """Conditional write as a distributed lock: only the first breach in the window wins,
    even across concurrent processor invocations."""
    item = {
        **_cooldown_key(alert),
        "alert_id": {"S": alert["alert_id"]},
        "expires_at": {"N": str(now + ALERT_COOLDOWN_SECONDS)},
        "ttl": {"N": str(now + ALERT_COOLDOWN_SECONDS + 3600)},
    }
    try:
        ddb.put_item(
            TableName=c.TABLE_NAME,
            Item=item,
            ConditionExpression="attribute_not_exists(pk) OR expires_at < :now",
            ExpressionAttributeValues={":now": {"N": str(now)}},
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def release_cooldown(alert):
    try:
        ddb.delete_item(
            TableName=c.TABLE_NAME,
            Key=_cooldown_key(alert),
            ConditionExpression="alert_id = :id",
            ExpressionAttributeValues={":id": {"S": alert["alert_id"]}},
        )
    except ClientError:
        log.warning("could not release cooldown for alert %s", alert["alert_id"])


def sns_entry(entry_id, alert):
    payload = {k: v for k, v in alert.items() if not k.startswith("_")}
    email = (
        f"{alert['severity'].upper()}: {alert['message']}\n\n"
        f"Event:   {alert['event_id']} (planner {alert['planner_id']})\n"
        f"Zone:    {alert['zone']} - sensor {alert['sensor_id']}\n"
        f"Reading: {alert['value']} {alert['metric']} at {alert['reading_ts']}\n"
        f"Alert:   {alert['alert_id']}\n"
    )
    return {
        "Id": entry_id,
        "Subject": _ascii(f"[{alert['severity'].upper()}] {alert['alert_type']} alert - {alert['event_id']} / {alert['zone']}")[:99],
        # Per-protocol bodies: email subscribers get readable text, the SQS queue
        # (and anything else) gets the JSON under "default".
        "MessageStructure": "json",
        "Message": json.dumps({"default": json.dumps(payload), "email": email}),
        # Subscription filter policies match on these (the email subscription only
        # takes severity = critical).
        "MessageAttributes": {
            "severity": {"DataType": "String", "StringValue": alert["severity"]},
            "alert_type": {"DataType": "String", "StringValue": alert["alert_type"]},
            "planner_id": {"DataType": "String", "StringValue": alert["planner_id"]},
        },
    }


def _ascii(text):
    """SNS subjects must be ASCII."""
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()


# --- data lake (archive_mode = lambda) ---

def archive(records):
    """Raw batch -> one GZIP'd JSON-lines object, in the same raw/dt=.../ layout Firehose
    uses. The key comes from the shard and first sequence number, so a retried batch
    overwrites its own file instead of duplicating it."""
    first = records[0]
    shard_id = first.get("eventID", "shard").split(":")[0]
    arrived = datetime.fromtimestamp(first["kinesis"].get("approximateArrivalTimestamp", time.time()), timezone.utc)
    lines = []
    for record in records:
        data = base64.b64decode(record["kinesis"]["data"])
        lines.append(data if data.endswith(b"\n") else data + b"\n")
    s3.put_object(
        Bucket=ARCHIVE_BUCKET,
        Key=f"raw/dt={arrived:%Y-%m-%d-%H}/{shard_id}-{first['kinesis']['sequenceNumber']}.json.gz",
        Body=gzip.compress(b"".join(lines)),
        ContentType="application/json",
    )


# --- custom metrics ---

def emit_metrics(processed, invalid, writes, alerts_published, latencies_ms):
    values = {
        "RecordsProcessed": (processed, "Count"),
        "InvalidRecords": (invalid, "Count"),
        "DynamoDBWrites": (writes, "Count"),
        "AlertsPublished": (alerts_published, "Count"),
    }
    if latencies_ms:
        step = max(1, len(latencies_ms) // 100)  # EMF takes up to 100 values per metric
        values["EndToEndLatencyMs"] = ([round(v) for v in latencies_ms[::step][:100]], "Milliseconds")
    c.emit_metrics(values)
