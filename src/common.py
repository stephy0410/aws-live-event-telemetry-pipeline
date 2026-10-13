"""Shared by the three Lambdas: single-table key layout, time buckets, DynamoDB marshalling.

Single-table layout (table `<name>-ops`):

  entity          pk                 sk                                gsi1pk             gsi1sk
  planner         PLANNERS           PLANNER#<planner_id>
  event           EVENT#<event_id>   META                              PLANNER#<pid>      EVENT#<event_id>
  zone state      EVENT#<event_id>   STATE#<zone>
  minute agg      EVENT#<event_id>   AGG#<yyyy-mm-ddThh:mm>#<zone>
  alert           EVENT#<event_id>   ALERT#<detected_at>#<alert_id>    ALERTS#<yyyy-mm-dd> <detected_at>#<alert_id>
  alert cooldown  EVENT#<event_id>   COOLDOWN#<zone>#<type>#<severity>
  platform stats  STATS              MINUTE#<yyyy-mm-ddThh:mm>
"""

import base64
import json
import math
import os
import re
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

TABLE_NAME = os.environ.get("TABLE_NAME", "")
METRICS_NAMESPACE = os.environ.get("METRICS_NAMESPACE", "EventOps/Pipeline")
PIPELINE_NAME = os.environ.get("PIPELINE_NAME", "local")

# --- data contract with producers (simulator/sensor_fleet.py, README "Data contract") ---

METRICS = {"occupancy", "temperature_c", "noise_db", "checkin"}
ID_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{1,64}$")  # ids end up in keys and SQL, so no '#' or quotes


class InvalidRecord(ValueError):
    pass


def decode(record):
    """One Kinesis record (as Lambda delivers it) -> validated message dict with a parsed
    `_dt` timestamp. Raises InvalidRecord. Every stream consumer uses this, so they all
    agree on what a valid record is."""
    try:
        msg = json.loads(base64.b64decode(record["kinesis"]["data"]))
    except (ValueError, KeyError, TypeError) as e:
        raise InvalidRecord(f"not JSON: {e}") from None
    if not isinstance(msg, dict):
        raise InvalidRecord("not a JSON object")

    kind = msg.get("kind")
    if kind == "sensor.reading":
        _require_ids(msg, "event_id", "planner_id", "zone", "sensor_id", "reading_id")
        if msg.get("metric") not in METRICS:
            raise InvalidRecord(f"unknown metric {msg.get('metric')!r}")
        value = msg.get("value")
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise InvalidRecord("value must be a finite number")
        if msg["metric"] == "occupancy":
            capacity = msg.get("capacity")
            if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity <= 0:
                raise InvalidRecord("occupancy readings need a positive integer capacity")
    elif kind == "event.registered":
        _require_ids(msg, "event_id", "planner_id")
        for field in ("planner_name", "event_name", "event_type"):
            if not isinstance(msg.get(field), str) or not msg[field]:
                raise InvalidRecord(f"missing {field}")
    else:
        raise InvalidRecord(f"unknown kind {kind!r}")

    try:
        msg["_dt"] = parse_ts(msg.get("ts") or "")
    except (ValueError, TypeError):
        raise InvalidRecord("ts must be ISO-8601 with a timezone") from None
    return msg


def _require_ids(msg, *fields):
    for field in fields:
        value = msg.get(field)
        if not isinstance(value, str) or not ID_PATTERN.match(value):
            raise InvalidRecord(f"missing or invalid {field}")

_serializer = TypeSerializer()
_deserializer = TypeDeserializer()


# --- keys ---

def event_pk(event_id):
    return f"EVENT#{event_id}"


def planner_gsi_pk(planner_id):
    return f"PLANNER#{planner_id}"


def alerts_gsi_pk(day):
    return f"ALERTS#{day}"


# --- time ---

def now_utc():
    return datetime.now(timezone.utc)


def parse_ts(value):
    """ISO-8601 string (with Z or offset) -> aware UTC datetime. Raises ValueError if invalid."""
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return dt.astimezone(timezone.utc)


def iso(dt):
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def minute_bucket(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M")


def day_bucket(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d")


def ttl_after(days, start=None):
    return int(((start or now_utc()) + timedelta(days=days)).timestamp())


# --- DynamoDB marshalling ---

def _to_dynamo_value(value):
    if isinstance(value, float):
        return Decimal(str(round(value, 3)))
    if isinstance(value, dict):
        return {k: _to_dynamo_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_dynamo_value(v) for v in value]
    return value


def marshal(value):
    return _serializer.serialize(_to_dynamo_value(value))


def marshal_item(item):
    return {k: marshal(v) for k, v in item.items() if v is not None}


def unmarshal_item(item):
    return {k: _deserializer.deserialize(v) for k, v in item.items()}


def key(pk, sk):
    return {"pk": {"S": pk}, "sk": {"S": sk}}


def update_request(pk, sk, set_values=None, add_values=None, set_if_absent=None):
    """Build UpdateItem kwargs. Every attribute name goes through a placeholder, so
    reserved words (zone, minute, value, ttl, ...) need no special care."""
    names, values, set_parts, add_parts = {}, {}, [], []

    def placeholders(attr, val):
        i = len(names)
        names[f"#a{i}"] = attr
        values[f":v{i}"] = marshal(val)
        return f"#a{i}", f":v{i}"

    for attr, val in (set_values or {}).items():
        if val is not None:
            n, v = placeholders(attr, val)
            set_parts.append(f"{n} = {v}")
    for attr, val in (set_if_absent or {}).items():
        n, v = placeholders(attr, val)
        set_parts.append(f"{n} = if_not_exists({n}, {v})")
    for attr, val in (add_values or {}).items():
        n, v = placeholders(attr, val)
        add_parts.append(f"{n} {v}")

    expression = []
    if set_parts:
        expression.append("SET " + ", ".join(set_parts))
    if add_parts:
        expression.append("ADD " + ", ".join(add_parts))

    return {
        "TableName": TABLE_NAME,
        "Key": key(pk, sk),
        "UpdateExpression": " ".join(expression),
        "ExpressionAttributeNames": names,
        "ExpressionAttributeValues": values,
    }


# --- custom metrics ---

def emit_metrics(values):
    """CloudWatch Embedded Metric Format: CloudWatch turns this log line into metrics
    (namespace METRICS_NAMESPACE, dimension Pipeline), with no PutMetricData calls.
    `values` maps metric name -> (value or list of up to 100 values, unit)."""
    if not os.environ.get("AWS_LAMBDA_FUNCTION_NAME"):
        return  # only meaningful inside Lambda (keeps tests and the local demo quiet)
    doc = {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [{
                "Namespace": METRICS_NAMESPACE,
                "Dimensions": [["Pipeline"]],
                "Metrics": [{"Name": name, "Unit": unit} for name, (_, unit) in values.items()],
            }],
        },
        "Pipeline": PIPELINE_NAME,
        **{name: value for name, (value, _) in values.items()},
    }
    print(json.dumps(doc))


# --- JSON ---

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return int(o) if o == o.to_integral_value() else float(o)
        if isinstance(o, set):
            return sorted(o)
        return super().default(o)


def dumps(obj):
    return json.dumps(obj, cls=DecimalEncoder, ensure_ascii=False, separators=(",", ":"))
