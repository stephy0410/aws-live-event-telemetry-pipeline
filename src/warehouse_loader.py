"""Kinesis -> Lambda -> Amazon Redshift: the warehouse behind the analytics Grafana dashboard.

A third consumer of the telemetry stream, tuned very differently from the others:
Lambda buffers up to 5 minutes (or 10,000 records) per invocation, because a warehouse
wants few, large loads, not one write per reading. Each invocation:

  1. aggregates its readings per (minute, event, zone, metric): count, sum, min, max
  2. loads them through the Redshift Data API (HTTPS + IAM: no VPC access, driver or
     password in the Lambda) in ONE transaction that first deletes the rows of the same
     batch_id, so a retried batch replaces itself instead of double counting
  3. refreshes the `events` dimension table from "event.registered" records

Invoked with {"action": "init_schema"} (Terraform does this once), it creates the
tables and the view Grafana queries.
"""

import logging
import math
import os
import time
from datetime import timezone

import boto3

import common as c

log = logging.getLogger()
log.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

DATABASE = os.environ.get("REDSHIFT_DATABASE", "")
WORKGROUP = os.environ.get("REDSHIFT_WORKGROUP", "")      # Redshift Serverless
CLUSTER_ID = os.environ.get("REDSHIFT_CLUSTER_ID", "")    # provisioned cluster
DB_USER = os.environ.get("REDSHIFT_DB_USER", "")          # provisioned only (temporary credentials)

MAX_STATEMENT_CHARS = 60_000  # the Data API limit is 100 KB per statement
POLL_SECONDS = 0.5

SCHEMA = [
    """CREATE TABLE IF NOT EXISTS telemetry_minute (
        batch_id    VARCHAR(160)     NOT NULL,
        minute_ts   TIMESTAMP        NOT NULL,
        planner_id  VARCHAR(64)      NOT NULL,
        event_id    VARCHAR(64)      NOT NULL,
        zone        VARCHAR(64)      NOT NULL,
        metric      VARCHAR(32)      NOT NULL,
        readings    INTEGER          NOT NULL,
        value_sum   DOUBLE PRECISION NOT NULL,
        value_min   DOUBLE PRECISION NOT NULL,
        value_max   DOUBLE PRECISION NOT NULL,
        capacity    INTEGER,
        loaded_at   TIMESTAMP DEFAULT SYSDATE
    )
    DISTSTYLE AUTO
    SORTKEY (minute_ts)""",
    """CREATE TABLE IF NOT EXISTS events (
        event_id        VARCHAR(64)  NOT NULL,
        planner_id      VARCHAR(64)  NOT NULL,
        planner_name    VARCHAR(256),
        event_name      VARCHAR(256),
        event_type      VARCHAR(32),
        venue           VARCHAR(256),
        city            VARCHAR(128),
        guests_expected INTEGER,
        starts_at       TIMESTAMP,
        registered_at   TIMESTAMP,
        PRIMARY KEY (event_id)
    )
    DISTSTYLE ALL""",
    # One row per minute, event, zone and metric, whichever loads the partial rows came from.
    """CREATE OR REPLACE VIEW event_metrics_minute AS
    SELECT t.minute_ts, t.planner_id, e.planner_name, t.event_id, e.event_name, e.event_type,
           t.zone, t.metric,
           SUM(t.readings)                              AS readings,
           SUM(t.value_sum)                             AS value_sum,
           SUM(t.value_sum) / NULLIF(SUM(t.readings), 0) AS value_avg,
           MIN(t.value_min)                             AS value_min,
           MAX(t.value_max)                             AS value_max,
           MAX(t.capacity)                              AS capacity
    FROM telemetry_minute t
    LEFT JOIN events e ON e.event_id = t.event_id
    GROUP BY t.minute_ts, t.planner_id, e.planner_name, t.event_id, e.event_name, e.event_type, t.zone, t.metric""",
]

rsd = boto3.client("redshift-data")


def handler(event, context):
    if event.get("action") == "init_schema":
        run(SCHEMA)
        log.info("schema ready")
        return {"schema": "ready"}

    started = time.time()
    records = event.get("Records", [])
    readings, registrations, invalid = [], {}, 0
    for record in records:
        try:
            msg = c.decode(record)
        except c.InvalidRecord:
            invalid += 1
            continue
        if msg["kind"] == "sensor.reading":
            readings.append(msg)
        else:
            registrations[msg["event_id"]] = msg  # latest registration per event wins

    rows = aggregate(readings)
    if records:
        batch_id = batch_identifier(records)
        statements = load_statements(batch_id, rows, list(registrations.values()))
        if statements:
            run(statements)

    load_ms = round((time.time() - started) * 1000)
    c.emit_metrics({"WarehouseRowsLoaded": (len(rows), "Count"), "WarehouseLoadMs": (load_ms, "Milliseconds")})
    summary = {"records": len(records), "rows": len(rows), "events": len(registrations), "invalid": invalid, "duration_ms": load_ms}
    log.info("batch loaded %s", summary)
    return summary


def batch_identifier(records):
    """Shard + first sequence number: the same batch always gets the same id."""
    first = records[0]
    return f"{first.get('eventID', 'shard').split(':')[0]}:{first['kinesis']['sequenceNumber']}"


def aggregate(readings):
    """(minute, planner, event, zone, metric) -> [count, sum, min, max, capacity]."""
    rows = {}
    for r in readings:
        key = (c.minute_bucket(r["_dt"]), r["planner_id"], r["event_id"], r["zone"], r["metric"])
        value = float(r["value"])
        row = rows.get(key)
        if row is None:
            rows[key] = [1, value, value, value, r.get("capacity")]
        else:
            row[0] += 1
            row[1] += value
            row[2] = min(row[2], value)
            row[3] = max(row[3], value)
            row[4] = row[4] or r.get("capacity")
    return rows


def load_statements(batch_id, rows, registrations):
    statements = [f"DELETE FROM telemetry_minute WHERE batch_id = {lit(batch_id)}"]
    values = [
        "(" + ", ".join([
            lit(batch_id), lit(f"{minute.replace('T', ' ')}:00"), lit(planner_id), lit(event_id), lit(zone), lit(metric),
            lit(count), lit(total), lit(low), lit(high), lit(capacity),
        ]) + ")"
        for (minute, planner_id, event_id, zone, metric), (count, total, low, high, capacity) in sorted(rows.items())
    ]
    statements += chunked_insert(
        "INSERT INTO telemetry_minute (batch_id, minute_ts, planner_id, event_id, zone, metric, "
        "readings, value_sum, value_min, value_max, capacity) VALUES ", values)

    if registrations:
        ids = ", ".join(lit(r["event_id"]) for r in registrations)
        statements.append(f"DELETE FROM events WHERE event_id IN ({ids})")
        values = [
            "(" + ", ".join([
                lit(r["event_id"]), lit(r["planner_id"]), lit(r.get("planner_name")), lit(r.get("event_name")),
                lit(r.get("event_type")), lit(r.get("venue")), lit(r.get("city")), lit(r.get("guests_expected")),
                lit(sql_timestamp(r.get("starts_at"))), lit(sql_timestamp(r.get("ts"))),
            ]) + ")"
            for r in registrations
        ]
        statements += chunked_insert(
            "INSERT INTO events (event_id, planner_id, planner_name, event_name, event_type, venue, city, "
            "guests_expected, starts_at, registered_at) VALUES ", values)

    return statements if len(statements) > 1 else []


def chunked_insert(prefix, values):
    statements, chunk, size = [], [], len(prefix)
    for v in values:
        if chunk and size + len(v) + 2 > MAX_STATEMENT_CHARS:
            statements.append(prefix + ", ".join(chunk))
            chunk, size = [], len(prefix)
        chunk.append(v)
        size += len(v) + 2
    if chunk:
        statements.append(prefix + ", ".join(chunk))
    return statements


def lit(value):
    """SQL literal. Batch statements can't take bind parameters, so strings are escaped:
    quotes doubled, backslashes and control characters dropped."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value) if math.isfinite(value) else "NULL"
    text = "".join(ch for ch in str(value) if ch >= " " and ch != "\\")
    return "'" + text.replace("'", "''") + "'"


def sql_timestamp(iso_value):
    if not iso_value:
        return None
    try:
        return c.parse_ts(iso_value).astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    except ValueError:
        return None


# --- Redshift Data API ---

def target():
    if WORKGROUP:
        return {"WorkgroupName": WORKGROUP, "Database": DATABASE}
    return {"ClusterIdentifier": CLUSTER_ID, "Database": DATABASE, "DbUser": DB_USER}


def run(statements):
    """All statements in one transaction (BatchExecuteStatement), then wait for the result.
    The Data API is asynchronous: it returns an id right away and runs the SQL in the background.
    The first call after a while can take longer: Redshift Serverless resumes on demand."""
    if len(statements) == 1:
        statement_id = rsd.execute_statement(Sql=statements[0], **target())["Id"]
    else:
        statement_id = rsd.batch_execute_statement(Sqls=statements, **target())["Id"]
    while True:
        status = rsd.describe_statement(Id=statement_id)
        if status["Status"] == "FINISHED":
            return status
        if status["Status"] in ("FAILED", "ABORTED"):
            raise RuntimeError(f"Redshift statement {statement_id} {status['Status']}: {status.get('Error')}")
        time.sleep(POLL_SECONDS)

