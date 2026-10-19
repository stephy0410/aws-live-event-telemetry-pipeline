"""End-to-end tests of the pipeline against mocked AWS (moto): no account or credentials needed.

  simulator records -> processor (as Lambda would call it from Kinesis) -> DynamoDB + SNS
  SNS -> SQS (real subscription in moto) -> alert worker -> DynamoDB
  DynamoDB -> dashboard API
  simulator records -> time-series writer -> Timestream
  simulator records -> warehouse loader -> Redshift Data API (SQL captured by a fake client)

Run with `make test`.
"""

import base64
import gzip
import importlib
import json
import os
import sys
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "simulator")]

os.environ.update({
    "AWS_DEFAULT_REGION": "us-east-1",
    "AWS_ACCESS_KEY_ID": "testing",
    "AWS_SECRET_ACCESS_KEY": "testing",
    "AWS_SESSION_TOKEN": "testing",
    "LOG_LEVEL": "CRITICAL",
})

import boto3  # noqa: E402
from moto import mock_aws  # noqa: E402

import sensor_fleet  # noqa: E402

TABLE = "eventops-ops"
BUCKET = "eventops-datalake-test"


def iso(dt):
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def kinesis_event(records, first_seq=1000):
    """What Lambda hands the function for a batch from one shard."""
    now = time.time()
    return {"Records": [
        {
            "eventID": f"shardId-000000000000:{first_seq + i}",
            "eventSource": "aws:kinesis",
            "kinesis": {
                "data": base64.b64encode(r if isinstance(r, bytes) else (json.dumps(r) + "\n").encode()).decode(),
                "partitionKey": r["event_id"] if isinstance(r, dict) and "event_id" in r else "x",
                "sequenceNumber": str(first_seq + i),
                "approximateArrivalTimestamp": now,
            },
        }
        for i, r in enumerate(records)
    ]}


def reading(event_id="evt-1", zone="ballroom", metric="occupancy", value=100, capacity=200, ts=None, **extra):
    r = {
        "kind": "sensor.reading", "reading_id": f"r-{time.monotonic_ns()}", "planner_id": "pl-1",
        "event_id": event_id, "zone": zone, "sensor_id": f"s-{zone}", "metric": metric, "value": value,
        "unit": "x", "ts": ts or iso(datetime.now(timezone.utc)),
    }
    if metric == "occupancy":
        r["capacity"] = capacity
    r.update(extra)
    return r


class PipelineTest(unittest.TestCase):
    def setUp(self):
        self.mock = mock_aws()
        self.mock.start()
        self.ddb = boto3.client("dynamodb")
        self.ddb.create_table(
            TableName=TABLE, BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[{"AttributeName": n, "AttributeType": "S"} for n in ("pk", "sk", "gsi1pk", "gsi1sk")],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
            GlobalSecondaryIndexes=[{
                "IndexName": "gsi1",
                "KeySchema": [{"AttributeName": "gsi1pk", "KeyType": "HASH"}, {"AttributeName": "gsi1sk", "KeyType": "RANGE"}],
                "Projection": {"ProjectionType": "ALL"},
            }],
        )
        sns = boto3.client("sns")
        sqs = boto3.client("sqs")
        self.topic_arn = sns.create_topic(Name="eventops-alerts")["TopicArn"]
        self.queue_url = sqs.create_queue(QueueName="eventops-alerts")["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(QueueUrl=self.queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        sns.subscribe(TopicArn=self.topic_arn, Protocol="sqs", Endpoint=queue_arn, Attributes={"RawMessageDelivery": "true"})
        self.sqs = sqs
        boto3.client("s3").create_bucket(Bucket=BUCKET)
        tsw = boto3.client("timestream-write")
        tsw.create_database(DatabaseName="eventops")
        tsw.create_table(DatabaseName="eventops", TableName="telemetry")

        os.environ.update({
            "TABLE_NAME": TABLE, "ALERTS_TOPIC_ARN": self.topic_arn, "ARCHIVE_BUCKET": "",
            "TIMESTREAM_DATABASE": "eventops", "TIMESTREAM_TABLE": "telemetry",
            "REDSHIFT_DATABASE": "eventops", "REDSHIFT_WORKGROUP": "eventops",
        })
        self.load_modules()

    def tearDown(self):
        self.mock.stop()

    def load_modules(self):
        # Reloaded inside the mock so their module-level clients and settings are fresh.
        import alert_worker
        import common
        import dashboard_api
        import processor
        import timeseries_writer
        import warehouse_loader
        self.common = importlib.reload(common)
        self.processor = importlib.reload(processor)
        self.worker = importlib.reload(alert_worker)
        self.api = importlib.reload(dashboard_api)
        self.writer = importlib.reload(timeseries_writer)
        self.loader = importlib.reload(warehouse_loader)

    def item(self, pk, sk):
        response = self.ddb.get_item(TableName=TABLE, Key={"pk": {"S": pk}, "sk": {"S": sk}})
        return self.common.unmarshal_item(response["Item"]) if "Item" in response else None

    def drain_queue(self):
        messages = []
        while True:
            batch = self.sqs.receive_message(QueueUrl=self.queue_url, MaxNumberOfMessages=10).get("Messages", [])
            if not batch:
                return messages
            messages += batch
            for m in batch:
                self.sqs.delete_message(QueueUrl=self.queue_url, ReceiptHandle=m["ReceiptHandle"])

    def api_get(self, route, headers=None, **path):
        response = self.api.handler({"routeKey": route, "pathParameters": path or None, "headers": headers or {}}, None)
        body = response["body"]
        return response["statusCode"], (json.loads(body) if response["headers"]["content-type"] == "application/json" else body)

    def api_headers(self, route, headers=None, **path):
        return self.api.handler({"routeKey": route, "pathParameters": path or None, "headers": headers or {}}, None)["headers"]

    # --- tests ---

    def test_simulator_records_satisfy_the_processor_contract(self):
        fleet = sensor_fleet.Fleet(events=12, seed=3, clock=0.0)
        records = [r for t in range(5) for r in fleet.tick(300, clock=float(t))]
        for record in kinesis_event(records)["Records"]:
            self.common.decode(record)  # raises InvalidRecord on any contract break
        kinds = {r["kind"] for r in records}
        self.assertEqual(kinds, {"event.registered", "sensor.reading"})

    def test_end_to_end_simulated_fleet(self):
        fleet = sensor_fleet.Fleet(events=8, seed=11, chaos=400, clock=0.0)  # chaos: force incidents
        sent, seq = 0, 0
        for t in range(0, 240, 2):
            records = fleet.tick(150, clock=float(t))
            for i in range(0, len(records), 500):
                chunk = records[i:i + 500]
                summary = self.processor.handler(kinesis_event(chunk, first_seq=seq), None)
                self.assertEqual(summary["invalid"], 0)
                sent += len(chunk)
                seq += len(chunk)

        # Platform stats count every record exactly once.
        stats = self.ddb.query(TableName=TABLE, KeyConditionExpression="pk = :p", ExpressionAttributeValues={":p": {"S": "STATS"}})["Items"]
        self.assertEqual(sum(int(s["records"]["N"]) for s in stats), sent)

        # Every event got registered under its planner, with live zone state and aggregates.
        for event in fleet.events:
            meta = self.item(f"EVENT#{event.event_id}", "META")
            self.assertEqual(meta["event_name"], event.event_name)
            self.assertEqual(meta["gsi1pk"], f"PLANNER#{event.planner_id}")
            self.assertEqual(int(meta["checked_in"]), event.checked_in)
            state = self.item(f"EVENT#{event.event_id}", f"STATE#{event.entrance}")
            self.assertIn("occupancy", state)
            self.assertIn("temperature_c", state)

        # Alerts went processor -> SNS -> SQS; the worker stores them idempotently.
        messages = self.drain_queue()
        self.assertGreater(len(messages), 0, "chaos should have produced alerts")
        sqs_event = {"Records": [{"messageId": m["MessageId"], "body": m["Body"]} for m in messages]}
        self.assertEqual(self.worker.handler(sqs_event, None), {"batchItemFailures": []})
        self.assertEqual(self.worker.handler(sqs_event, None), {"batchItemFailures": []})  # redelivery
        alert = json.loads(messages[0]["Body"])
        stored = self.ddb.query(
            TableName=TABLE, KeyConditionExpression="pk = :p AND begins_with(sk, :a)",
            ExpressionAttributeValues={":p": {"S": f"EVENT#{alert['event_id']}"}, ":a": {"S": "ALERT#"}},
        )["Items"]
        self.assertTrue(any(i["alert_id"]["S"] == alert["alert_id"] for i in stored))
        meta = self.item(f"EVENT#{alert['event_id']}", "META")
        stored_for_event = sum(1 for m in messages if json.loads(m["Body"])["event_id"] == alert["event_id"])
        self.assertEqual(int(meta["alerts_total"]), stored_for_event)  # duplicates not counted

        # Dashboard API reads it all back.
        status, page = self.api_get("GET /")
        self.assertEqual(status, 200)
        self.assertIn('"occupancy_warning_ratio": 0.9', page)
        status, overview = self.api_get("GET /api/overview")
        self.assertEqual(status, 200)
        self.assertEqual({p["planner_id"] for p in overview["planners"]}, {e.planner_id for e in fleet.events})
        self.assertEqual(sum(t["records"] for t in overview["throughput"]), sent)
        self.assertEqual(len(overview["throughput"]), 30)
        self.assertGreater(len(overview["alerts"]), 0)
        self.assertIn("event_name", overview["alerts"][0])
        self.assertIn("recommended_action", overview["alerts"][0])

        planner = fleet.events[0].planner_id
        status, body = self.api_get("GET /api/planners/{planner_id}/events", planner_id=planner)
        self.assertEqual(status, 200)
        self.assertEqual({e["event_id"] for e in body["events"]}, {e.event_id for e in fleet.events if e.planner_id == planner})
        self.assertTrue(all(e["live"] for e in body["events"]))

        status, detail = self.api_get("GET /api/events/{event_id}", event_id=fleet.events[0].event_id)
        self.assertEqual(status, 200)
        self.assertEqual([z["zone"] for z in detail["zones"]], list(fleet.events[0].zones))
        self.assertTrue(any("occupancy" in p for p in detail["series"]))

    def test_invalid_records_are_counted_and_skipped(self):
        good = reading()
        bad = [
            b"not json at all",
            {"kind": "sensor.reading", "event_id": "evt-1"},                          # missing fields
            reading(metric="humidity"),                                               # unknown metric
            reading(value="12"),                                                      # value not a number
            reading(capacity=None),                                                   # occupancy without capacity
            reading(event_id="evt#1"),                                                # '#' would break the key layout
            reading(ts="2026-10-13T18:00:00"),                                        # no timezone
            {"kind": "something.else", "event_id": "evt-1"},
        ]
        summary = self.processor.handler(kinesis_event([good, *bad]), None)
        self.assertEqual(summary["readings"], 1)
        self.assertEqual(summary["invalid"], len(bad))

    def test_alert_rules(self):
        evaluate = self.processor.evaluate
        cases = [
            (reading(value=179, capacity=200), None),
            (reading(value=180, capacity=200), ("capacity", "warning")),
            (reading(value=200, capacity=200), ("capacity", "critical")),
            (reading(zone="cold-room", metric="temperature_c", value=5.0), None),
            (reading(zone="cold-room", metric="temperature_c", value=5.4), ("cold_chain", "critical")),
            (reading(metric="temperature_c", value=27.9), None),
            (reading(metric="temperature_c", value=28.5), ("comfort", "warning")),
            (reading(metric="noise_db", value=94.9), None),
            (reading(metric="noise_db", value=96), ("noise", "warning")),
            (reading(metric="noise_db", value=106), ("noise", "critical")),
            (reading(metric="checkin", value=500), None),
        ]
        for r, expected in cases:
            r["_dt"] = datetime.now(timezone.utc)
            result = evaluate(r)
            self.assertEqual(result[:2] if result else None, expected, r)

    def test_cooldown_publishes_each_alert_once_and_keeps_the_worst_severity(self):
        first = self.processor.handler(kinesis_event([reading(value=185), reading(value=210)]), None)
        self.assertEqual(first["alerts_published"], 1)  # warning + critical in one batch -> just the critical
        second = self.processor.handler(kinesis_event([reading(value=215)], first_seq=2000), None)
        self.assertEqual(second["alerts_published"], 0)  # same zone/type/severity inside the cooldown
        bodies = [json.loads(m["Body"]) for m in self.drain_queue()]
        self.assertEqual([b["severity"] for b in bodies], ["critical"])

    def test_alert_worker_reports_only_bad_messages_as_failures(self):
        good = {
            "alert_id": "a-1", "event_id": "evt-1", "planner_id": "pl-1", "zone": "bar", "alert_type": "noise",
            "severity": "warning", "message": "bar is loud", "detected_at": iso(datetime.now(timezone.utc)), "value": 97.5,
        }
        envelope = {"Type": "Notification", "Message": json.dumps({**good, "alert_id": "a-2"})}
        result = self.worker.handler({"Records": [
            {"messageId": "m1", "body": json.dumps(good)},
            {"messageId": "m2", "body": "{broken"},
            {"messageId": "m3", "body": json.dumps(envelope)},  # subscription without raw delivery
        ]}, None)
        self.assertEqual(result, {"batchItemFailures": [{"itemIdentifier": "m2"}]})
        self.assertEqual(int(self.item("EVENT#evt-1", "META")["alerts_total"]), 2)

    def test_archive_mode_lambda_writes_the_raw_batch_to_s3(self):
        os.environ["ARCHIVE_BUCKET"] = BUCKET
        self.load_modules()
        records = [reading(), reading(metric="noise_db", value=70), b"garbage"]
        event = kinesis_event(records)
        self.processor.handler(event, None)
        self.processor.handler(event, None)  # a retried batch overwrites the same object

        s3 = boto3.client("s3")
        objects = s3.list_objects_v2(Bucket=BUCKET)["Contents"]
        self.assertEqual(len(objects), 1)
        key = objects[0]["Key"]
        hour = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H")
        self.assertRegex(key, rf"^raw/dt={hour}/shardId-000000000000-1000\.json\.gz$")
        lines = gzip.decompress(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()).splitlines()
        self.assertEqual(len(lines), 3)  # raw means raw: invalid records are archived too

    def test_dashboard_rejects_bad_ids_and_unknown_events(self):
        self.assertEqual(self.api_get("GET /api/events/{event_id}", event_id="evt#1")[0], 400)
        self.assertEqual(self.api_get("GET /api/events/{event_id}", event_id="evt-missing")[0], 404)
        self.assertEqual(self.api_get("GET /api/planners/{planner_id}/events", planner_id="a b")[0], 400)
        self.assertEqual(self.api_get("GET /nope")[0], 404)

    def test_dashboard_cache_serves_hits_without_dynamodb(self):
        self.processor.handler(kinesis_event([reading()]), None)
        overview = "GET /api/overview"

        first = self.api_headers(overview)
        self.assertEqual(first["x-cache"], "MISS")
        self.assertGreater(int(first["x-dynamodb-calls"]), 0)
        self.assertIn("app;dur=", first["server-timing"])

        second = self.api_headers(overview)
        self.assertEqual((second["x-cache"], second["x-dynamodb-calls"]), ("HIT", "0"))

        # Cache-Control: no-cache goes to DynamoDB even with a fresh entry (load-test baseline).
        bypass = self.api_headers(overview, headers={"cache-control": "no-cache"})
        self.assertEqual(bypass["x-cache"], "BYPASS")
        self.assertGreater(int(bypass["x-dynamodb-calls"]), 0)

        # Entries expire after the TTL; keys are per route and id; errors are never cached.
        self.api._cache["GET /api/overview"] = (time.monotonic() - 1, "{}")
        self.assertEqual(self.api_headers(overview)["x-cache"], "MISS")
        event = "GET /api/events/{event_id}"
        self.assertEqual(self.api_headers(event, event_id="evt-1")["x-cache"], "MISS")
        self.assertEqual(self.api_headers(event, event_id="evt-1")["x-cache"], "HIT")
        self.assertEqual(self.api_headers(event, event_id="evt-missing")["x-cache"], "MISS")
        self.assertEqual(self.api_headers(event, event_id="evt-missing")["x-cache"], "MISS")

    def test_dashboard_cache_can_be_turned_off(self):
        os.environ["API_CACHE_TTL_SECONDS"] = "0"
        try:
            self.load_modules()
            self.api_get("GET /api/overview")
            self.assertEqual(self.api_headers("GET /api/overview")["x-cache"], "MISS")
        finally:
            del os.environ["API_CACHE_TTL_SECONDS"]

    def test_timeseries_writer_writes_every_reading(self):
        fleet = sensor_fleet.Fleet(events=6, seed=5, clock=0.0)
        records = [r for t in range(3) for r in fleet.tick(250, clock=float(t))]
        readings = [r for r in records if r["kind"] == "sensor.reading"]
        summary = self.writer.handler(kinesis_event(records + [b"garbage"]), None)
        self.assertEqual(summary["written"], len(readings))
        self.assertEqual(summary["rejected"], 0)
        self.assertEqual(summary["invalid"], 1)

        record = self.writer.to_timestream(self.common.decode(kinesis_event([readings[0]])["Records"][0]))
        self.assertEqual([d["Name"] for d in record["Dimensions"]], ["planner_id", "event_id", "zone", "sensor_id"])
        self.assertEqual(record["MeasureName"], readings[0]["metric"])
        self.assertEqual(float(record["MeasureValue"]), float(readings[0]["value"]))

    def test_warehouse_loader_builds_one_idempotent_transaction(self):
        calls = []

        class FakeDataApi:
            def execute_statement(self, **kw):
                calls.append(("single", [kw["Sql"]], kw))
                return {"Id": "s1"}

            def batch_execute_statement(self, **kw):
                calls.append(("batch", kw["Sqls"], kw))
                return {"Id": "b1"}

            def describe_statement(self, Id):
                return {"Id": Id, "Status": "FINISHED"}

        self.loader.rsd = FakeDataApi()
        self.assertEqual(self.loader.handler({"action": "init_schema"}, None), {"schema": "ready"})
        self.assertEqual(len(calls[0][1]), 3)  # two tables and the Grafana view, in one transaction

        minute = datetime.now(timezone.utc).replace(second=1, microsecond=0)
        registration = {
            "kind": "event.registered", "event_id": "evt-1", "planner_id": "pl-1", "planner_name": "Planner O'Hara",
            "event_name": "Boda D'Angelo \\ test", "event_type": "wedding", "guests_expected": 120,
            "starts_at": "2026-10-13T18:00:00Z", "ts": iso(minute),
        }
        records = [registration] + [reading(value=v, ts=iso(minute + timedelta(seconds=i))) for i, v in enumerate((100, 140, 120))]
        records.append(reading(metric="noise_db", value=91.5, ts=iso(minute)))
        event = kinesis_event(records, first_seq=777)
        summary = self.loader.handler(event, None)
        self.assertEqual((summary["rows"], summary["events"]), (2, 1))

        kind, sqls, target = calls[-1]
        self.assertEqual(kind, "batch")
        self.assertEqual(target["WorkgroupName"], "eventops")
        self.assertEqual(sqls[0], "DELETE FROM telemetry_minute WHERE batch_id = 'shardId-000000000000:777'")
        insert = sqls[1]
        self.assertIn(f"'{minute:%Y-%m-%d %H:%M}:00', 'pl-1', 'evt-1', 'ballroom', 'occupancy', 3, 360.0, 100.0, 140.0, 200)", insert)
        self.assertIn("'noise_db', 1, 91.5, 91.5, 91.5, NULL)", insert)
        self.assertEqual(sqls[2], "DELETE FROM events WHERE event_id IN ('evt-1')")
        self.assertIn("'Planner O''Hara'", sqls[3])  # quotes doubled
        self.assertIn("'Boda D''Angelo  test'", sqls[3])  # backslashes dropped

        self.loader.handler(event, None)  # a retried batch deletes its own rows first
        self.assertEqual(calls[-1][1][0], sqls[0])

    def test_aggregates_average_per_zone_and_minute(self):
        minute = datetime.now(timezone.utc).replace(second=5, microsecond=0)
        records = [reading(value=v, ts=iso(minute + timedelta(seconds=i))) for i, v in enumerate((100, 110, 120))]
        self.processor.handler(kinesis_event(records), None)
        agg = self.item("EVENT#evt-1", f"AGG#{minute:%Y-%m-%dT%H:%M}#ballroom")
        self.assertEqual(int(agg["occupancy_count"]), 3)
        self.assertEqual(float(agg["occupancy_sum"]), 330.0)
        state = self.item("EVENT#evt-1", "STATE#ballroom")
        self.assertEqual(int(state["occupancy"]), 120)  # latest reading wins


if __name__ == "__main__":
    unittest.main()
