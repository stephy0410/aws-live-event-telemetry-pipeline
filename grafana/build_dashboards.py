"""Generates the Grafana dashboards in grafana/dashboards/ (run `make dashboards`).

The JSON files are committed, so Terraform needs nothing but them. To change a
dashboard either edit it here and regenerate, or edit it in Grafana (sign in as admin)
and export the JSON over the file.

Placeholders replaced by Terraform at deploy time:
  __NAME__ resource prefix, __COLD_ROOM_MAX_C__, __NOISE_WARNING_DB__, __NOISE_CRITICAL_DB__,
  __OCCUPANCY_WARNING_PCT__
"""
import json
import sys
from pathlib import Path

OUT = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent / "dashboards"
OUT.mkdir(parents=True, exist_ok=True)

GOLD, IVORY, TAUPE, ROSE, SAGE, SLATE = "#C9A86A", "#E8E2D6", "#A39382", "#C98B8B", "#8FA58C", "#7F93A8"
TS = {"type": "grafana-timestream-datasource", "uid": "timestream"}
RS = {"type": "grafana-redshift-datasource", "uid": "redshift"}
CW = {"type": "cloudwatch", "uid": "cloudwatch"}

_id = 0


def pid():
    global _id
    _id += 1
    return _id


def thresholds(*steps):
    """steps: (value, color) after the base."""
    return {"mode": "absolute", "steps": [{"color": GOLD, "value": None}] + [{"color": c, "value": v} for v, c in steps]}


def panel(kind, title, x, y, w, h, targets, ds, desc="", unit=None, fc=None, options=None, extra=None):
    defaults = {"color": {"mode": "palette-classic"}, "custom": {}}
    if unit:
        defaults["unit"] = unit
    if fc:
        defaults.update(fc)
    p = {
        "id": pid(), "type": kind, "title": title, "description": desc, "datasource": ds,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": targets,
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": options or {},
    }
    if extra:
        p.update(extra)
    return p


def timeseries_options(legend="list"):
    return {"legend": {"displayMode": legend, "placement": "bottom", "showLegend": True},
            "tooltip": {"mode": "multi", "sort": "desc"}}


def line_custom(fill=8, draw="line", bars=False):
    c = {"drawStyle": "bars" if bars else draw, "lineWidth": 2, "fillOpacity": fill, "pointSize": 4,
         "showPoints": "never", "spanNulls": True, "lineInterpolation": "smooth", "gradientMode": "opacity"}
    return c


def stat(title, x, y, w, target, ds, unit=None, desc="", color=GOLD):
    return panel("stat", title, x, y, w, 4, [target], ds, desc=desc, unit=unit,
                 fc={"color": {"mode": "fixed", "fixedColor": color}},
                 options={"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                          "colorMode": "value", "graphMode": "none", "textMode": "value", "justifyMode": "center"})


def row(title, y):
    return {"id": pid(), "type": "row", "title": title, "collapsed": False, "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []}


def dashboard(uid, title, description, panels, templating=None, refresh="30s", time_from="now-30m", tags=()):
    return {
        "uid": uid, "title": title, "description": description, "tags": ["eventops", *tags],
        "timezone": "browser", "editable": True, "graphTooltip": 1, "schemaVersion": 39, "version": 1,
        "refresh": refresh, "time": {"from": time_from, "to": "now"},
        "timepicker": {"refresh_intervals": ["10s", "30s", "1m", "5m"]},
        "templating": {"list": templating or []}, "annotations": {"list": []},
        "links": [{"title": "EventOps dashboards", "type": "dashboards", "tags": ["eventops"], "asDropdown": True}],
        "panels": panels,
    }


# ---------------------------------------------------------------- live (Timestream)

def ts_target(sql, ref="A", fmt=0):
    # Timestream plugin: format 0 = Table (long -> wide), 1 = Time series.
    return {"refId": ref, "datasource": TS, "rawQuery": sql, "format": fmt}


def zone_series(measure, extra_where=""):
    return ts_target(f"""SELECT zone, CREATE_TIME_SERIES(t, v) AS {measure}
FROM (
  SELECT zone, bin(time, $__interval_ms) AS t, avg(measure_value::double) AS v
  FROM $__database.$__table
  WHERE $__timeFilter AND measure_name = '{measure}' AND event_id = '$event'{extra_where}
  GROUP BY zone, bin(time, $__interval_ms)
)
GROUP BY zone
ORDER BY zone""")


_id = 0
live_panels = [
    stat("Readings in range", 0, 0, 6, ts_target("SELECT count(*) AS readings FROM $__database.$__table WHERE $__timeFilter"), TS,
         unit="short", desc="Sensor readings stored in Timestream for the selected time range"),
    stat("Readings per second", 6, 0, 6, ts_target("SELECT count(*) / 60.0 AS per_second FROM $__database.$__table WHERE time > ago(1m)"), TS,
         unit="short", desc="Average over the last minute"),
    stat("Live events", 12, 0, 6, ts_target("SELECT count(DISTINCT event_id) AS live FROM $__database.$__table WHERE time > ago(2m)"), TS,
         desc="Events with at least one reading in the last 2 minutes", color=IVORY),
    stat("Guests checked in · $event", 18, 0, 6,
         ts_target("SELECT sum(measure_value::double) AS guests FROM $__database.$__table WHERE measure_name = 'checkin' AND event_id = '$event' AND time > ago(1d)"),
         TS, desc="All check-ins of the selected event", color=IVORY),
    panel("timeseries", "Readings per minute by planner", 0, 4, 24, 8, [ts_target("""SELECT planner_id, CREATE_TIME_SERIES(t, n) AS readings
FROM (
  SELECT planner_id, bin(time, 1m) AS t, count(*) AS n
  FROM $__database.$__table
  WHERE $__timeFilter
  GROUP BY planner_id, bin(time, 1m)
)
GROUP BY planner_id
ORDER BY planner_id""")], TS, unit="short", desc="Platform throughput per tenant: how much telemetry each planner's events produce",
          fc={"displayName": "${__field.labels.planner_id}", "custom": line_custom(fill=12, bars=True)}, options=timeseries_options()),
    row("Event · $event", 12),
    panel("timeseries", "Occupancy by zone", 0, 13, 12, 9, [zone_series("occupancy", " AND zone <> 'cold-room'")], TS, unit="short",
          desc="People in each zone (door counters), averaged per interval",
          fc={"displayName": "${__field.labels.zone}", "custom": line_custom()}, options=timeseries_options()),
    panel("timeseries", "Temperature by zone", 12, 13, 12, 9, [zone_series("temperature_c")], TS, unit="celsius",
          desc="The cold-room line must stay under the food-safety limit",
          fc={"displayName": "${__field.labels.zone}", "custom": {**line_custom(fill=0), "thresholdsStyle": {"mode": "line"}},
              "thresholds": thresholds(("__COLD_ROOM_MAX_C__", ROSE))},
          options=timeseries_options()),
    panel("timeseries", "Noise by zone", 0, 22, 12, 9, [zone_series("noise_db", " AND zone <> 'cold-room'")], TS, unit="dB",
          desc="Warning and venue-limit levels drawn as lines",
          fc={"displayName": "${__field.labels.zone}", "custom": {**line_custom(fill=0), "thresholdsStyle": {"mode": "line"}},
              "thresholds": thresholds(("__NOISE_WARNING_DB__", TAUPE), ("__NOISE_CRITICAL_DB__", ROSE))},
          options=timeseries_options()),
    panel("timeseries", "Check-ins per minute", 12, 22, 12, 9, [ts_target("""SELECT 'check-ins' AS series, CREATE_TIME_SERIES(t, n) AS guests
FROM (
  SELECT bin(time, 1m) AS t, sum(measure_value::double) AS n
  FROM $__database.$__table
  WHERE $__timeFilter AND measure_name = 'checkin' AND event_id = '$event'
  GROUP BY bin(time, 1m)
)""")], TS, unit="short", desc="Guests scanned at the entrance",
          fc={"displayName": "Check-ins", "color": {"mode": "fixed", "fixedColor": GOLD}, "custom": line_custom(fill=60, bars=True)},
          options=timeseries_options()),
    panel("table", "Latest reading per zone", 0, 31, 24, 9, [ts_target("""SELECT zone, measure_name AS metric,
       round(max_by(measure_value::double, time), 1) AS value,
       max(time) AS reported_at
FROM $__database.$__table
WHERE event_id = '$event' AND time > ago(15m) AND measure_name <> 'checkin'
GROUP BY zone, measure_name
ORDER BY zone, metric""")], TS, desc="Most recent value of every sensor in the selected event",
          options={"showHeader": True, "cellHeight": "sm", "footer": {"show": False}}),
]
live = dashboard("eventops-live", "EventOps · Live telemetry (Timestream)",
                 "Kinesis -> Lambda -> Amazon Timestream. Near real time, one point per sensor reading.",
                 live_panels, refresh="10s", time_from="now-30m", tags=("timestream",), templating=[{
                     "name": "event", "label": "Event", "type": "query", "datasource": TS,
                     "query": "SELECT DISTINCT event_id FROM $__database.$__table WHERE time > ago(2h) ORDER BY event_id",
                     "definition": "SELECT DISTINCT event_id FROM $__database.$__table WHERE time > ago(2h) ORDER BY event_id",
                     "refresh": 2, "sort": 1, "includeAll": False, "multi": False, "current": {}, "options": [],
                 }])

# ---------------------------------------------------------------- warehouse (Redshift)

def rs_target(sql, ref="A", fmt=1):
    # Redshift plugin: format 0 = Time series (long -> wide), 1 = Table.
    return {"refId": ref, "datasource": RS, "rawSQL": sql, "format": fmt, "fillMode": {"mode": 0}}


_id = 0
warehouse_panels = [
    stat("Readings loaded in range", 0, 0, 6, rs_target("SELECT SUM(readings) AS readings FROM telemetry_minute WHERE $__timeFilter(minute_ts)"), RS, unit="short"),
    stat("Rows in the warehouse", 6, 0, 6, rs_target("SELECT COUNT(*) AS rows_total FROM telemetry_minute"), RS, unit="short",
         desc="One row per minute, event, zone and metric per load", color=IVORY),
    stat("Events registered", 12, 0, 6, rs_target("SELECT COUNT(*) AS events FROM events"), RS, color=IVORY),
    stat("Last load", 18, 0, 6, rs_target("SELECT MAX(loaded_at) AS last_load FROM telemetry_minute"), RS, unit="dateTimeFromNow",
         desc="Loads arrive in micro-batches, every few minutes"),
    panel("timeseries", "Readings per minute by event type", 0, 4, 24, 8,
          [rs_target("""SELECT minute_ts AS time, COALESCE(event_type, 'unregistered') AS event_type, SUM(readings) AS readings
FROM event_metrics_minute
WHERE $__timeFilter(minute_ts)
GROUP BY 1, 2
ORDER BY 1""", fmt=0)], RS, unit="short",
          fc={"displayName": "${__field.labels.event_type}", "custom": line_custom(fill=12, bars=True)}, options=timeseries_options()),
    panel("bargauge", "Peak occupancy by zone, % of capacity", 0, 12, 12, 11,
          [rs_target("""SELECT COALESCE(event_name, event_id) || ' · ' || zone AS zone,
       ROUND(100.0 * MAX(value_max) / NULLIF(MAX(capacity), 0), 1) AS peak_pct
FROM event_metrics_minute
WHERE metric = 'occupancy' AND zone <> 'cold-room' AND $__timeFilter(minute_ts)
GROUP BY 1
ORDER BY 2 DESC
LIMIT 12""")], RS, unit="percent", desc="Fullest minute of each zone",
          fc={"min": 0, "max": 120, "color": {"mode": "thresholds"},
              "thresholds": thresholds(("__OCCUPANCY_WARNING_PCT__", TAUPE), (100, ROSE))},
          options={"orientation": "horizontal", "displayMode": "basic", "showUnfilled": True, "valueMode": "text",
                   "reduceOptions": {"calcs": [], "fields": "", "values": True}, "namePlacement": "left"}),
    panel("table", "Check-ins by event", 12, 12, 12, 11,
          [rs_target("""SELECT e.planner_name AS planner, e.event_name AS event, e.event_type AS type,
       e.guests_expected AS expected, SUM(t.value_sum)::int AS checked_in,
       ROUND(100.0 * SUM(t.value_sum) / NULLIF(e.guests_expected, 0), 0) AS pct
FROM telemetry_minute t
JOIN events e ON e.event_id = t.event_id
WHERE t.metric = 'checkin' AND $__timeFilter(t.minute_ts)
GROUP BY 1, 2, 3, 4
ORDER BY checked_in DESC""")], RS, desc="Guests scanned at the entrance vs. the guest list",
          fc={"custom": {"align": "auto"}},
          options={"showHeader": True, "cellHeight": "sm", "footer": {"show": False}}),
    panel("timeseries", "Cold-room temperature, max per minute", 0, 23, 12, 9,
          [rs_target("""SELECT minute_ts AS time, COALESCE(event_name, event_id) AS event, MAX(value_max) AS celsius
FROM event_metrics_minute
WHERE metric = 'temperature_c' AND zone = 'cold-room' AND $__timeFilter(minute_ts)
GROUP BY 1, 2
ORDER BY 1""", fmt=0)], RS, unit="celsius", desc="Food safety across every event",
          fc={"displayName": "${__field.labels.event}", "custom": {**line_custom(fill=0), "thresholdsStyle": {"mode": "line"}},
              "thresholds": thresholds(("__COLD_ROOM_MAX_C__", ROSE))}, options=timeseries_options("table")),
    panel("table", "Loudest zones", 12, 23, 12, 9,
          [rs_target("""SELECT COALESCE(event_name, event_id) AS event, zone,
       COUNT(*) AS loud_minutes, ROUND(MAX(value_max), 1) AS peak_db
FROM event_metrics_minute
WHERE metric = 'noise_db' AND value_max >= __NOISE_WARNING_DB__ AND $__timeFilter(minute_ts)
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 10""")], RS, desc="Minutes at or above the noise warning level",
          options={"showHeader": True, "cellHeight": "sm", "footer": {"show": False}}),
]
warehouse = dashboard("eventops-warehouse", "EventOps · Analytics (Redshift)",
                      "Kinesis -> Lambda (5-minute micro-batches) -> Amazon Redshift. History and comparisons across planners and events.",
                      warehouse_panels, refresh="1m", time_from="now-3h", tags=("redshift",))

# ---------------------------------------------------------------- pipeline (CloudWatch)

def cw(ref, namespace, metric, dims, stat="Sum", label=None):
    return {"refId": ref, "datasource": CW, "queryMode": "Metrics", "region": "default", "namespace": namespace,
            "metricName": metric, "dimensions": dims, "statistic": stat, "period": "60", "metricQueryType": 0,
            "metricEditorMode": 0, "matchExact": True, "id": "", "expression": "", "label": label or metric}


def emf(ref, metric, stat="Sum", label=None):
    return cw(ref, "EventOps/Pipeline", metric, {"Pipeline": "__NAME__"}, stat, label)


def fn(name):
    return f"__NAME__-{name}"


CONSUMERS = [("stream-processor", "Processor → DynamoDB"), ("timeseries-writer", "Writer → Timestream"), ("warehouse-loader", "Loader → Redshift")]
FUNCTIONS = CONSUMERS + [("alert-worker", "Alert worker"), ("dashboard-api", "Dashboard API")]
REFS = "ABCDEFGHIJ"

_id = 0
pipeline_panels = [
    panel("timeseries", "Ingestion · records into Kinesis per minute", 0, 0, 12, 8,
          [cw("A", "AWS/Kinesis", "IncomingRecords", {"StreamName": fn("telemetry")}, label="Incoming records"),
           cw("B", "AWS/Kinesis", "WriteProvisionedThroughputExceeded", {"StreamName": fn("telemetry")}, label="Throttled writes")],
          CW, unit="short", fc={"custom": line_custom(fill=10)}, options=timeseries_options()),
    panel("timeseries", "Processing lag · iterator age per consumer", 12, 0, 12, 8,
          [cw(REFS[i], "AWS/Lambda", "IteratorAge", {"FunctionName": fn(f)}, "Maximum", label) for i, (f, label) in enumerate(CONSUMERS)],
          CW, unit="ms", desc="How far behind the newest record each consumer is. The warehouse loader waits up to 5 minutes on purpose.",
          fc={"custom": line_custom(fill=0)}, options=timeseries_options()),
    panel("timeseries", "Delivered to each store per minute", 0, 8, 12, 8,
          [emf("A", "RecordsProcessed", label="DynamoDB (processor)"),
           emf("B", "TimestreamRecordsWritten", label="Timestream"),
           emf("C", "WarehouseRowsLoaded", label="Redshift rows (aggregated)"),
           cw("D", "AWS/Firehose", "DeliveryToS3.Records", {"DeliveryStreamName": fn("archive")}, label="S3 data lake (Firehose)")],
          CW, unit="short", desc="Custom metrics emitted by the Lambdas with CloudWatch Embedded Metric Format",
          fc={"custom": line_custom(fill=0)}, options=timeseries_options()),
    panel("timeseries", "Sensor → DynamoDB latency", 12, 8, 12, 8,
          [emf("A", "EndToEndLatencyMs", "p50", "p50"), emf("B", "EndToEndLatencyMs", "p95", "p95"), emf("C", "EndToEndLatencyMs", "Maximum", "max")],
          CW, unit="ms", desc="From the sensor's timestamp to the live state being written",
          fc={"custom": line_custom(fill=0)}, options=timeseries_options()),
    panel("timeseries", "Lambda invocations", 0, 16, 12, 8,
          [cw(REFS[i], "AWS/Lambda", "Invocations", {"FunctionName": fn(f)}, label=label) for i, (f, label) in enumerate(FUNCTIONS)],
          CW, unit="short", fc={"custom": line_custom(fill=0)}, options=timeseries_options()),
    panel("timeseries", "Lambda errors (should stay at 0)", 12, 16, 12, 8,
          [cw(REFS[i], "AWS/Lambda", "Errors", {"FunctionName": fn(f)}, label=label) for i, (f, label) in enumerate(FUNCTIONS)],
          CW, unit="short", fc={"custom": line_custom(fill=0), "color": {"mode": "palette-classic"}}, options=timeseries_options()),
    panel("timeseries", "Alerts · topic → queue → worker", 0, 24, 12, 8,
          [emf("A", "AlertsPublished", label="Published by processor"),
           cw("B", "AWS/SNS", "NumberOfMessagesPublished", {"TopicName": fn("alerts")}, label="SNS published"),
           cw("C", "AWS/SQS", "NumberOfMessagesDeleted", {"QueueName": fn("alerts")}, label="Stored by worker")],
          CW, unit="short", fc={"custom": line_custom(fill=0)}, options=timeseries_options()),
    panel("timeseries", "Backlogs and dead-letter queues (should stay at 0)", 12, 24, 12, 8,
          [cw("A", "AWS/SQS", "ApproximateNumberOfMessagesVisible", {"QueueName": fn("alerts")}, "Maximum", "Alerts waiting"),
           cw("B", "AWS/SQS", "ApproximateNumberOfMessagesVisible", {"QueueName": fn("alerts-dlq")}, "Maximum", "Alerts DLQ"),
           cw("C", "AWS/SQS", "ApproximateNumberOfMessagesVisible", {"QueueName": fn("stream-dlq")}, "Maximum", "Stream DLQ")],
          CW, unit="short", fc={"custom": line_custom(fill=0)}, options=timeseries_options()),
    panel("timeseries", "DynamoDB capacity consumed", 0, 32, 24, 7,
          [cw("A", "AWS/DynamoDB", "ConsumedWriteCapacityUnits", {"TableName": fn("ops")}, label="Write units"),
           cw("B", "AWS/DynamoDB", "ConsumedReadCapacityUnits", {"TableName": fn("ops")}, label="Read units")],
          CW, unit="short", fc={"custom": line_custom(fill=10)}, options=timeseries_options()),
]
pipeline = dashboard("eventops-pipeline", "EventOps · Pipeline health (CloudWatch)",
                     "One stream, many consumers: ingestion, lag, delivery to each store, alerts and dead-letter queues.",
                     pipeline_panels, refresh="1m", time_from="now-1h", tags=("cloudwatch",))

for name, d in (("eventops-live.json", live), ("eventops-warehouse.json", warehouse), ("eventops-pipeline.json", pipeline)):
    text = json.dumps(d, indent=2, ensure_ascii=False)
    # Threshold placeholders become bare numbers once Terraform substitutes them.
    for ph in ("__COLD_ROOM_MAX_C__", "__NOISE_WARNING_DB__", "__NOISE_CRITICAL_DB__", "__OCCUPANCY_WARNING_PCT__"):
        text = text.replace(f'"{ph}"', ph)
    (OUT / name).write_text(text + "\n", encoding="utf-8")
    print(name, len(text))
