#!/usr/bin/env python3
"""Run the whole pipeline on your laptop against mocked AWS (moto) and serve the dashboard.

No AWS account or credentials needed: handy to work on the dashboard, or to demo the
project while the Learner Lab session is down.

  make local-demo            # then open http://localhost:8080

What runs, in one process:
  sensor fleet -> processor handler (fed batches shaped like Lambda's Kinesis events)
  -> DynamoDB + SNS -> SQS (moto) -> alert worker handler
  dashboard API handler, served over HTTP with API Gateway-shaped events

The same handler code is deployed to Lambda unchanged; only the AWS endpoints are fake.
"""

import argparse
import base64
import json
import os
import re
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlparse

ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "simulator")]

for k, v in {"AWS_DEFAULT_REGION": "us-east-1", "AWS_ACCESS_KEY_ID": "local", "AWS_SECRET_ACCESS_KEY": "local"}.items():
    os.environ.setdefault(k, v)
os.environ.update({"TABLE_NAME": "eventops-ops", "LOG_LEVEL": "WARNING", "ARCHIVE_BUCKET": ""})

import boto3  # noqa: E402
from moto import mock_aws  # noqa: E402

from sensor_fleet import Fleet  # noqa: E402

ROUTES = [
    (re.compile(r"^/$"), "GET /", ()),
    (re.compile(r"^/api/overview$"), "GET /api/overview", ()),
    (re.compile(r"^/api/planners/([^/]+)/events$"), "GET /api/planners/{planner_id}/events", ("planner_id",)),
    (re.compile(r"^/api/events/([^/]+)$"), "GET /api/events/{event_id}", ("event_id",)),
]


def create_resources(table_name):
    """The same table, topic, queue and subscription Terraform creates."""
    ddb = boto3.client("dynamodb")
    ddb.create_table(
        TableName=table_name, BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[{"AttributeName": n, "AttributeType": "S"} for n in ("pk", "sk", "gsi1pk", "gsi1sk")],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
        GlobalSecondaryIndexes=[{
            "IndexName": "gsi1",
            "KeySchema": [{"AttributeName": "gsi1pk", "KeyType": "HASH"}, {"AttributeName": "gsi1sk", "KeyType": "RANGE"}],
            "Projection": {"ProjectionType": "ALL"},
        }],
    )
    sns, sqs = boto3.client("sns"), boto3.client("sqs")
    topic_arn = sns.create_topic(Name="eventops-alerts")["TopicArn"]
    queue_url = sqs.create_queue(QueueName="eventops-alerts")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn, Attributes={"RawMessageDelivery": "true"})
    return topic_arn, queue_url


def kinesis_batch(records, first_seq, arrived):
    return {"Records": [{
        "eventID": f"shardId-000000000000:{first_seq + i}",
        "kinesis": {
            "data": base64.b64encode((json.dumps(r, ensure_ascii=False) + "\n").encode()).decode(),
            "partitionKey": r["event_id"],
            "sequenceNumber": str(first_seq + i),
            "approximateArrivalTimestamp": arrived,
        },
    } for i, r in enumerate(records)]}


class Pipeline:
    def __init__(self, args, queue_url):
        import alert_worker
        import processor
        self.processor, self.worker = processor, alert_worker
        self.sqs = boto3.client("sqs")
        self.queue_url = queue_url
        self.args = args
        self.lock = threading.Lock()  # moto's in-memory backends are not built for concurrent writers
        self.seq = 0
        self.fleet = Fleet(events=args.events, cycle_seconds=args.cycle, chaos=args.chaos,
                           clock=time.monotonic() - args.backfill_minutes * 60)

    def ingest(self, records, at):
        """One 'tick' through the pipeline: processor batches, then drain the alert queue into the worker."""
        with self.lock:
            for i in range(0, len(records), 1000):
                chunk = records[i:i + 1000]
                self.processor.handler(kinesis_batch(chunk, self.seq, at), None)
                self.seq += len(chunk)
            while True:
                messages = self.sqs.receive_message(QueueUrl=self.queue_url, MaxNumberOfMessages=10).get("Messages", [])
                if not messages:
                    break
                self.worker.handler({"Records": [{"messageId": m["MessageId"], "body": m["Body"]} for m in messages]}, None)
                for m in messages:
                    self.sqs.delete_message(QueueUrl=self.queue_url, ReceiptHandle=m["ReceiptHandle"])

    def backfill(self):
        """Pre-load the last few minutes so the charts are not empty on first load."""
        seconds = self.args.backfill_minutes * 60
        start_clock, now = time.monotonic() - seconds, time.time()
        for s in range(0, seconds, 2):
            at = now - seconds + s
            records = self.fleet.tick(self.args.rate * 2, clock=start_clock + s)
            stamp = datetime.fromtimestamp(at, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
            for r in records:
                r["ts"] = stamp
            self.ingest(records, at)

    def run(self):
        while True:
            started = time.time()
            self.ingest(self.fleet.tick(self.args.rate), started)
            time.sleep(max(0.0, 1.0 - (time.time() - started)))


def serve(pipeline, port):
    import dashboard_api

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            path = urlparse(self.path).path
            route, params = f"GET {path}", None
            for pattern, key, names in ROUTES:
                match = pattern.match(path)
                if match:
                    route, params = key, {n: unquote(v) for n, v in zip(names, match.groups())} or None
                    break
            with pipeline.lock:
                response = dashboard_api.handler({"routeKey": route, "pathParameters": params}, None)
            body = response["body"].encode()
            self.send_response(response["statusCode"])
            for k, v in response["headers"].items():
                self.send_header(k, v)
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"dashboard: http://localhost:{port}   (Ctrl+C to stop)")
    server.serve_forever()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--rate", type=int, default=20, help="sensor readings per second (default 20; moto is slower than AWS)")
    ap.add_argument("--events", type=int, default=12)
    ap.add_argument("--cycle", type=int, default=900, help="seconds per event timeline (default 900)")
    ap.add_argument("--chaos", type=float, default=1.5, help="incident frequency multiplier (default 1.5)")
    ap.add_argument("--backfill-minutes", type=int, default=12, help="history to pre-load (default 12)")
    args = ap.parse_args()

    with mock_aws():
        topic_arn, queue_url = create_resources(os.environ["TABLE_NAME"])
        os.environ["ALERTS_TOPIC_ARN"] = topic_arn
        pipeline = Pipeline(args, queue_url)
        print(f"pre-loading {args.backfill_minutes} minutes of telemetry...")
        pipeline.backfill()
        threading.Thread(target=pipeline.run, daemon=True).start()
        try:
            serve(pipeline, args.port)
        except KeyboardInterrupt:
            print("\nstopped")


if __name__ == "__main__":
    main()
