"""SQS -> Lambda alert worker.

The processor publishes alerts to the SNS topic; the topic fans them out to this
queue (and optionally to email). The worker enriches each alert with the event and
planner names and a recommended action, and stores it in DynamoDB for the alert feed.

SQS delivers at least once, so the write is conditional on the alert not existing
yet: a redelivered message is acknowledged without touching the counters again.
Messages that fail 3 times move to the dead-letter queue (redrive policy).
"""

import json
import logging
import os

import boto3
from botocore.exceptions import ClientError

import common as c

log = logging.getLogger()
log.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

AGGREGATE_TTL_DAYS = int(os.environ.get("AGGREGATE_TTL_DAYS", "7"))
ALERT_TTL_DAYS = int(os.environ.get("ALERT_TTL_DAYS", "30"))

PLAYBOOK = {
    ("capacity", "warning"): "Open the overflow area and slow down entry to this zone.",
    ("capacity", "critical"): "Stop entry now and redirect guests: the zone is over its safe capacity.",
    ("cold_chain", "critical"): "Check the cold-room door and compressor, move perishables and notify the caterer.",
    ("comfort", "warning"): "Turn up the air conditioning or open ventilation in this zone.",
    ("noise", "warning"): "Ask the DJ or band to lower the volume.",
    ("noise", "critical"): "Cut the volume now: the venue noise limit is exceeded.",
}
DEFAULT_ACTION = "Check the zone and follow the event's runbook."
REQUIRED_FIELDS = ("alert_id", "event_id", "planner_id", "zone", "alert_type", "severity", "message", "detected_at")

ddb = boto3.client("dynamodb")


def handler(event, context):
    failures = []
    for record in event.get("Records", []):
        try:
            store(parse(record["body"]))
        except Exception:
            log.exception("could not store alert from message %s", record.get("messageId"))
            failures.append({"itemIdentifier": record["messageId"]})
    # Partial batch response: only the failed messages become visible again.
    return {"batchItemFailures": failures}


def parse(body):
    alert = json.loads(body)
    # With raw message delivery off, the alert arrives wrapped in an SNS envelope.
    if alert.get("Type") == "Notification" and "Message" in alert:
        alert = json.loads(alert["Message"])
    missing = [f for f in REQUIRED_FIELDS if not alert.get(f)]
    if missing:
        raise ValueError(f"alert is missing {missing}")
    return alert


def store(alert):
    detected = c.parse_ts(alert["detected_at"])
    event_pk = c.event_pk(alert["event_id"])
    item = {
        **alert,
        **event_names(event_pk),
        "pk": event_pk,
        "sk": f"ALERT#{alert['detected_at']}#{alert['alert_id']}",
        "gsi1pk": c.alerts_gsi_pk(c.day_bucket(detected)),
        "gsi1sk": f"{alert['detected_at']}#{alert['alert_id']}",
        "recommended_action": PLAYBOOK.get((alert["alert_type"], alert["severity"]), DEFAULT_ACTION),
        "stored_at": c.iso(c.now_utc()),
        "ttl": c.ttl_after(ALERT_TTL_DAYS),
    }
    try:
        ddb.put_item(TableName=c.TABLE_NAME, Item=c.marshal_item(item), ConditionExpression="attribute_not_exists(pk)")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            log.info("duplicate delivery of alert %s ignored", alert["alert_id"])
            return
        raise

    minute = c.minute_bucket(detected)
    ddb.update_item(**c.update_request(
        event_pk, "META",
        set_values={
            "last_alert_at": alert["detected_at"],
            "last_alert": alert["message"],
            "last_alert_severity": alert["severity"],
        },
        add_values={"alerts_total": 1, f"alerts_{alert['severity']}": 1},
    ))
    ddb.update_item(**c.update_request(
        "STATS", f"MINUTE#{minute}",
        set_if_absent={"minute": minute, "ttl": c.ttl_after(AGGREGATE_TTL_DAYS)},
        add_values={"alerts": 1},
    ))
    log.info("stored %s %s alert %s for %s/%s", alert["severity"], alert["alert_type"], alert["alert_id"], alert["event_id"], alert["zone"])


def event_names(event_pk):
    """Enrichment: the processor only knows ids; the dashboard and emails want names."""
    response = ddb.get_item(
        TableName=c.TABLE_NAME,
        Key=c.key(event_pk, "META"),
        ProjectionExpression="event_name, planner_name",
    )
    return c.unmarshal_item(response.get("Item", {}))
