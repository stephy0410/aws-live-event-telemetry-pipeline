"""Kinesis -> Lambda -> Amazon Timestream: the time-series store behind the live Grafana dashboard.

An independent consumer of the telemetry stream: it keeps its own position in each
shard, its own retries and its own failure destination, so a Timestream problem never
slows down the DynamoDB path (and vice versa).

Every sensor reading becomes one Timestream record:
  dimensions  planner_id, event_id, zone, sensor_id
  measure     <metric> (occupancy, temperature_c, noise_db, checkin) = value, DOUBLE
  time        the sensor's own timestamp, in milliseconds

Writes are idempotent: when a batch is retried, Timestream accepts the identical
records again instead of duplicating them.
"""

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.config import Config

import common as c

log = logging.getLogger()
log.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

DATABASE = os.environ.get("TIMESTREAM_DATABASE", "")
TABLE = os.environ.get("TIMESTREAM_TABLE", "")
MAX_RECORDS_PER_WRITE = 100  # WriteRecords limit
DIMENSIONS = ("planner_id", "event_id", "zone", "sensor_id")

tsw = boto3.client("timestream-write", config=Config(
    read_timeout=20, max_pool_connections=16, retries={"mode": "standard", "max_attempts": 10},
))
_pool = ThreadPoolExecutor(max_workers=8)


def handler(event, context):
    started = time.time()
    records, invalid = [], 0
    for record in event.get("Records", []):
        try:
            msg = c.decode(record)
        except c.InvalidRecord:
            invalid += 1  # the stream processor logs the details; here we just skip it
            continue
        if msg["kind"] == "sensor.reading":
            records.append(to_timestream(msg))

    chunks = [records[i:i + MAX_RECORDS_PER_WRITE] for i in range(0, len(records), MAX_RECORDS_PER_WRITE)]
    # Throttling and server errors raise after the SDK's retries, so Lambda retries the batch.
    results = list(_pool.map(write, chunks))
    written = sum(w for w, _ in results)
    rejected = sum(r for _, r in results)

    c.emit_metrics({
        "TimestreamRecordsWritten": (written, "Count"),
        "TimestreamRecordsRejected": (rejected, "Count"),
    })
    summary = {
        "records": len(event.get("Records", [])),
        "written": written,
        "rejected": rejected,
        "invalid": invalid,
        "duration_ms": round((time.time() - started) * 1000),
    }
    log.info("batch written %s", summary)
    return summary


def to_timestream(reading):
    return {
        "Dimensions": [{"Name": d, "Value": reading[d]} for d in DIMENSIONS],
        "MeasureName": reading["metric"],
        "MeasureValue": repr(float(reading["value"])),
        "MeasureValueType": "DOUBLE",
        "Time": str(int(reading["_dt"].timestamp() * 1000)),
        "TimeUnit": "MILLISECONDS",
    }


def write(records):
    """-> (written, rejected). Rejected records (e.g. a timestamp outside the memory-store
    window) are a data problem, not an outage: log them and move on instead of retrying."""
    try:
        tsw.write_records(DatabaseName=DATABASE, TableName=TABLE, Records=records)
        return len(records), 0
    except tsw.exceptions.RejectedRecordsException as e:
        rejected = e.response.get("RejectedRecords", [])
        log.warning("%d of %d records rejected, first: %s", len(rejected), len(records), rejected[:1])
        return len(records) - len(rejected), len(rejected)
