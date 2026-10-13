#!/usr/bin/env python3
"""Sensor fleet simulator: live-event telemetry -> Amazon Kinesis Data Streams.

Simulates the devices an event-operations platform collects data from while several
planners (tenants) run weddings, conferences, festivals and galas at the same time:

  occupancy      people counters on every zone door   (value = people, with zone capacity)
  temperature_c  climate sensors, incl. the catering cold room
  noise_db       sound-level meters
  checkin        QR scanners at the entrance          (value = guests scanned since last report)

Each event runs a compressed timeline (arrival -> main program -> party -> wind-down)
over --cycle seconds; when one ends, the planner starts a new one. Incidents are
injected at random (crowd rush, cold-room door left open, noise spike, A/C failure)
so the alert path has something to catch.

Records use event_id as the Kinesis partition key: all readings of one event land on
the same shard, in order.

Examples:
  python3 simulator/sensor_fleet.py --stream eventops-telemetry                          # 100 records/s for 5 min
  python3 simulator/sensor_fleet.py --stream eventops-telemetry --rate 1000 --duration 120  # ingestion burst
  python3 simulator/sensor_fleet.py --dry-run --total 20                                 # print records, send nothing
"""

import argparse
import json
import math
import random
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

PLANNERS = {
    "pl-aurora": ("Aurora Weddings", ["wedding", "wedding", "gala"]),
    "pl-summit": ("Summit Conferences", ["conference", "conference", "gala"]),
    "pl-fiesta": ("Fiesta Total Eventos", ["festival", "wedding", "festival"]),
    "pl-gala": ("Gala Corporate Events", ["gala", "conference", "gala"]),
}

PHASES = ("arrival", "main", "party", "wind_down")

# zone: (capacity, share of the guests present that are in this zone, per phase)
EVENT_TYPES = {
    "wedding": {
        "guests": (120, 260),
        "zones": {
            "entrance": (60, (0.18, 0.03, 0.02, 0.08)),
            "ceremony-garden": (220, (0.55, 0.10, 0.02, 0.05)),
            "ballroom": (240, (0.05, 0.71, 0.33, 0.45)),
            "dance-floor": (140, (0.00, 0.10, 0.46, 0.22)),
            "bar": (60, (0.05, 0.05, 0.16, 0.18)),
            "cold-room": (4, None),
        },
        "names": ["Boda Ana & Luis", "Boda Sofía & Diego", "Boda Valeria & Andrés", "Boda Camila & Mateo",
                  "Boda Regina & Pablo", "Boda Lucía & Emilio", "Boda Mariana & Tomás", "Boda Paula & Rodrigo"],
        "venues": [("Hacienda San Gabriel", "Guadalajara"), ("Jardín Los Arcos", "Tlaquepaque"), ("Casa de los Patios", "Zapopan"),
                   ("Quinta Real", "Guadalajara")],
    },
    "conference": {
        "guests": (400, 900),
        "zones": {
            "entrance": (150, (0.14, 0.03, 0.03, 0.10)),
            "main-hall": (700, (0.20, 0.65, 0.30, 0.25)),
            "breakout-a": (250, (0.05, 0.12, 0.22, 0.05)),
            "breakout-b": (250, (0.05, 0.12, 0.22, 0.05)),
            "expo-floor": (600, (0.30, 0.06, 0.15, 0.50)),
            "cold-room": (6, None),
        },
        "names": ["Tech Summit GDL 2026", "Cumbre de Innovación Fintech", "Congreso Nacional de Ingeniería",
                  "Foro de Ciberseguridad", "Expo Agro Jalisco"],
        "venues": [("Expo Guadalajara", "Guadalajara"), ("Centro Citibanamex", "CDMX")],
    },
    "festival": {
        "guests": (2000, 4500),
        "zones": {
            "gate": (600, (0.10, 0.03, 0.02, 0.08)),
            "main-stage": (4000, (0.30, 0.55, 0.77, 0.40)),
            "food-court": (1500, (0.25, 0.28, 0.12, 0.28)),
            "vip-lounge": (400, (0.05, 0.07, 0.07, 0.07)),
            "cold-room": (10, None),
        },
        "names": ["Festival Sonoro 2026", "Noche de Jazz al Aire Libre", "Fest Electrónica GDL", "Feria del Mariachi"],
        "venues": [("Foro Sol", "CDMX"), ("Parque Metropolitano", "Guadalajara")],
    },
    "gala": {
        "guests": (150, 320),
        "zones": {
            "entrance": (70, (0.18, 0.03, 0.02, 0.10)),
            "reception": (200, (0.50, 0.10, 0.12, 0.30)),
            "dining-hall": (300, (0.05, 0.80, 0.50, 0.45)),
            "stage": (120, (0.00, 0.05, 0.32, 0.10)),
            "cold-room": (5, None),
        },
        "names": ["Gala Anual Fundación Esperanza", "Cena de Fin de Año Corporativa", "Premios a la Excelencia",
                  "Gala de Beneficencia Hospital Civil"],
        "venues": [("Hotel Camino Real", "Guadalajara"), ("Museo Cabañas", "Guadalajara")],
    },
}

LOUD_ZONES = {"dance-floor": 10, "stage": 10, "main-stage": 12, "bar": 4}
QUIET_ZONES = {"entrance": -4, "gate": -2, "breakout-a": -6, "breakout-b": -6, "ceremony-garden": -3}
NOISE_BY_PHASE = (60, 68, 84, 72)

INCIDENTS = ("crowd_rush", "cold_room_door", "noise_spike", "hvac_failure")
UNITS = {"occupancy": "people", "temperature_c": "celsius", "noise_db": "dB", "checkin": "guests"}
SENSOR_PREFIX = {"occupancy": "occ", "temperature_c": "tmp", "noise_db": "snd", "checkin": "qr"}


def iso_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class Zone:
    def __init__(self, name, capacity, shares):
        self.name = name
        self.capacity = capacity
        self.shares = shares  # None = staff-only zone (cold room)
        self.occupancy = 0.0
        self.temperature = 3.2 if name == "cold-room" else 22.0
        self.noise = 55.0


class LiveEvent:
    """One event and the state of its sensors."""

    def __init__(self, rng, planner_id, event_type, event_id, name, venue, cycle_seconds, start_progress, clock):
        spec = EVENT_TYPES[event_type]
        self.rng = rng
        self.planner_id = planner_id
        self.planner_name = PLANNERS[planner_id][0]
        self.event_type = event_type
        self.event_id = event_id
        self.event_name = name
        self.venue, self.city = venue
        self.guests_expected = rng.randint(*spec["guests"])
        self.show_rate = rng.uniform(0.86, 0.97)
        self.cycle = cycle_seconds
        self.started = clock - start_progress * cycle_seconds
        self.starts_at = datetime.fromtimestamp(time.time() - start_progress * cycle_seconds, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        self.zones = {name: Zone(name, cap, shares) for name, (cap, shares) in spec["zones"].items()}
        self.entrance = next(iter(self.zones))
        self.checked_in = 0
        self.incidents = {}  # kind -> (zone, until)
        self.sensors = [(zone, metric) for zone in self.zones for metric in ("occupancy", "temperature_c", "noise_db")
                        if not (zone == "cold-room" and metric == "noise_db")]

    # --- timeline ---

    def progress(self, clock):
        return (clock - self.started) / self.cycle

    def ended(self, clock):
        return self.progress(clock) >= 1.0

    def phase(self, clock):
        p = self.progress(clock)
        return 0 if p < 0.25 else 1 if p < 0.55 else 2 if p < 0.85 else 3

    def present(self, clock):
        p = self.progress(clock)
        leaving = 1.0 if p < 0.85 else max(0.15, 1 - (p - 0.85) / 0.15 * 0.85)
        return self.checked_in * leaving

    def maybe_start_incident(self, clock, chaos):
        if self.rng.random() >= 0.002 * chaos:
            return
        kind = self.rng.choice(INCIDENTS)
        if kind in self.incidents:
            return
        guest_zones = [z for z in self.zones if z != "cold-room"]
        zone = "cold-room" if kind == "cold_room_door" else self.rng.choice(guest_zones)
        self.incidents[kind] = (zone, clock + self.rng.uniform(60, 150))

    def incident(self, kind, zone, clock):
        active = self.incidents.get(kind)
        if active and active[1] < clock:
            del self.incidents[kind]
            return False
        return bool(active) and active[0] == zone

    # --- records ---

    def registration(self):
        return {
            "kind": "event.registered",
            "event_id": self.event_id,
            "planner_id": self.planner_id,
            "planner_name": self.planner_name,
            "event_name": self.event_name,
            "event_type": self.event_type,
            "venue": self.venue,
            "city": self.city,
            "guests_expected": self.guests_expected,
            "starts_at": self.starts_at,
            "zones": [{"zone": z.name, "capacity": z.capacity} for z in self.zones.values()],
            "ts": iso_now(),
        }

    def checkin(self, clock):
        """Guests scanned at the entrance since the last report (None if nobody arrived)."""
        p = min(self.progress(clock), 1.0)
        arrived = round(self.guests_expected * self.show_rate * (1 - math.exp(-p / 0.09)))
        delta = arrived - self.checked_in
        if delta <= 0:
            return None
        self.checked_in = arrived
        return self._reading(self.entrance, "checkin", delta)

    def read(self, zone_name, metric, clock):
        zone = self.zones[zone_name]
        phase = self.phase(clock)
        g = self.rng.gauss

        if metric == "occupancy":
            if zone.shares is None:
                zone.occupancy = self.rng.choice((0, 0, 1, 1, 2))
            else:
                boost = 1.6 if self.incident("crowd_rush", zone_name, clock) else 1.0
                target = self.present(clock) * zone.shares[phase] * boost
                zone.occupancy += (target - zone.occupancy) * 0.35 + g(0, 1 + target * 0.02)
                zone.occupancy = min(max(zone.occupancy, 0), zone.capacity * 1.4)
            return self._reading(zone_name, metric, int(round(zone.occupancy)), capacity=zone.capacity)

        if metric == "temperature_c":
            if zone_name == "cold-room":
                if self.incident("cold_room_door", zone_name, clock):
                    zone.temperature = min(zone.temperature + 0.35, 9.0)
                else:
                    zone.temperature += (3.2 - zone.temperature) * 0.2 + g(0, 0.08)
            else:
                hvac = 6.5 if self.incident("hvac_failure", zone_name, clock) else 0.0
                target = 22.0 + 4.5 * zone.occupancy / zone.capacity + hvac
                zone.temperature += (target - zone.temperature) * 0.3 + g(0, 0.15)
            return self._reading(zone_name, metric, round(zone.temperature, 2))

        # noise_db
        loud = LOUD_ZONES.get(zone_name, 0) if phase == 2 else 0
        spike = 13 if self.incident("noise_spike", zone_name, clock) else 0
        target = NOISE_BY_PHASE[phase] + loud + QUIET_ZONES.get(zone_name, 0) + spike
        zone.noise += (target - zone.noise) * 0.4 + g(0, 1.8)
        return self._reading(zone_name, metric, round(zone.noise, 1))

    def _reading(self, zone, metric, value, capacity=None):
        record = {
            "kind": "sensor.reading",
            "reading_id": str(uuid.uuid4()),
            "planner_id": self.planner_id,
            "event_id": self.event_id,
            "zone": zone,
            "sensor_id": f"{SENSOR_PREFIX[metric]}-{zone}-01",
            "metric": metric,
            "value": value,
            "unit": UNITS[metric],
            "ts": iso_now(),
        }
        if capacity is not None:
            record["capacity"] = capacity
        return record


class Fleet:
    """All events across all planners; produces the records for one tick at a time."""

    REGISTRATION_EVERY = 60  # seconds; re-sent so consumers that missed one catch up

    def __init__(self, events=12, cycle_seconds=900, chaos=1.0, seed=None, run_id=None, clock=None):
        self.rng = random.Random(seed)
        self.cycle = cycle_seconds
        self.chaos = chaos
        self.run_id = run_id or datetime.now(timezone.utc).strftime("%m%d%H%M")
        self.counter = 0
        self.editions = {}  # event type -> how many events of that type were created
        self.names = {t: self.rng.sample(spec["names"], len(spec["names"])) for t, spec in EVENT_TYPES.items()}
        clock = time.monotonic() if clock is None else clock
        slots = [(pid, etype) for pid, (_, types) in PLANNERS.items() for etype in types]
        self.slots = [slots[i % len(slots)] for i in range(events)]
        # Stagger the start so that, at any moment, events are in different phases.
        self.events = [self._new_event(pid, etype, clock, self.rng.uniform(0, 0.7)) for pid, etype in self.slots]
        self.sensors = [(e, zone, metric) for e in self.events for zone, metric in e.sensors]
        self.rng.shuffle(self.sensors)
        self.cursor = 0
        self.last_registration = None

    def _new_event(self, planner_id, event_type, clock, start_progress):
        """Names and venues rotate per type, so events running at the same time never share a name."""
        self.counter += 1
        n = self.editions[event_type] = self.editions.get(event_type, 0) + 1
        names, venues = self.names[event_type], EVENT_TYPES[event_type]["venues"]
        name = names[(n - 1) % len(names)]
        if n > len(names):
            name += f" ({(n - 1) // len(names) + 1})"
        event_id = f"evt-{self.run_id}-{event_type}-{self.counter:02d}"
        return LiveEvent(self.rng, planner_id, event_type, event_id, name, venues[(n - 1) % len(venues)],
                         self.cycle, start_progress, clock)

    def tick(self, readings, clock=None):
        """Records for one tick: `readings` sensor readings (round-robin over every sensor),
        plus check-ins and, when due, event registrations."""
        clock = time.monotonic() if clock is None else clock
        records = []

        replaced = False
        for i, event in enumerate(self.events):
            if event.ended(clock):
                self.events[i] = self._new_event(event.planner_id, event.event_type, clock, 0.0)
                records.append(self.events[i].registration())
                replaced = True
        if replaced:
            self.sensors = [(e, zone, metric) for e in self.events for zone, metric in e.sensors]
            self.rng.shuffle(self.sensors)

        if self.last_registration is None or clock - self.last_registration >= self.REGISTRATION_EVERY:
            records += [e.registration() for e in self.events]
            self.last_registration = clock

        for event in self.events:
            event.maybe_start_incident(clock, self.chaos)
            checkin = event.checkin(clock)
            if checkin:
                records.append(checkin)

        for _ in range(readings):
            event, zone, metric = self.sensors[self.cursor % len(self.sensors)]
            self.cursor += 1
            records.append(event.read(zone, metric, clock))
        return records


class KinesisSender:
    """PutRecords in batches of up to 500, retrying only the records that failed
    (throttling on a hot shard fails individual records, not the whole call)."""

    MAX_BATCH = 500
    MAX_ATTEMPTS = 6

    def __init__(self, stream, profile, region):
        import boto3  # only needed when actually sending

        session = boto3.Session(profile_name=profile or None, region_name=region)
        self.kinesis = session.client("kinesis")
        self.stream = stream
        self.sent = 0
        self.throttled = 0
        self.failed = 0

    def check_stream(self):
        summary = self.kinesis.describe_stream_summary(StreamName=self.stream)["StreamDescriptionSummary"]
        return summary["StreamStatus"], summary.get("OpenShardCount")

    def send(self, records):
        for i in range(0, len(records), self.MAX_BATCH):
            self._send_batch(records[i:i + self.MAX_BATCH])

    def _send_batch(self, records):
        entries = [{"Data": (json.dumps(r, ensure_ascii=False) + "\n").encode(), "PartitionKey": r["event_id"]} for r in records]
        for attempt in range(self.MAX_ATTEMPTS):
            response = self.kinesis.put_records(StreamName=self.stream, Records=entries)
            if response["FailedRecordCount"] == 0:
                self.sent += len(entries)
                return
            retry = []
            for entry, result in zip(entries, response["Records"]):
                if "ErrorCode" in result:
                    retry.append(entry)
                    if result["ErrorCode"] == "ProvisionedThroughputExceededException":
                        self.throttled += 1
            self.sent += len(entries) - len(retry)
            entries = retry
            time.sleep(min(0.1 * 2 ** attempt, 2.0) * random.uniform(0.5, 1.0))  # exponential backoff + jitter
        self.failed += len(entries)


def terraform_output(name):
    try:
        root = Path(__file__).resolve().parent.parent
        return subprocess.run(["terraform", "output", "-raw", name], cwd=root, capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def parse_args():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--stream", help="Kinesis stream name (default: terraform output kinesis_stream_name)")
    ap.add_argument("--profile", default="academy", help="AWS CLI profile (empty = default credential chain)")
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--rate", type=int, default=100, help="sensor readings per second (default 100)")
    ap.add_argument("--duration", type=int, default=300, help="seconds to run (default 300)")
    ap.add_argument("--total", type=int, help="stop after this many records instead")
    ap.add_argument("--events", type=int, default=12, help="events running at the same time (default 12)")
    ap.add_argument("--cycle", type=int, default=900, help="seconds one event takes from arrival to wind-down (default 900)")
    ap.add_argument("--chaos", type=float, default=1.0, help="incident frequency multiplier, 0 = none (default 1)")
    ap.add_argument("--seed", type=int, help="random seed, for repeatable runs")
    ap.add_argument("--dry-run", action="store_true", help="print records as JSON lines instead of sending them")
    return ap.parse_args()


def main():
    args = parse_args()
    fleet = Fleet(events=args.events, cycle_seconds=args.cycle, chaos=args.chaos, seed=args.seed)

    if args.dry_run:
        total = args.total or 20
        printed = 0
        while printed < total:
            for record in fleet.tick(min(args.rate, total))[: total - printed]:
                print(json.dumps(record, ensure_ascii=False))
                printed += 1
        return

    stream = args.stream or terraform_output("kinesis_stream_name")
    if not stream:
        sys.exit("No stream given and `terraform output kinesis_stream_name` failed. Deploy first or pass --stream.")
    sender = KinesisSender(stream, args.profile, args.region)
    status, shards = sender.check_stream()
    print(f"stream {stream}: {status}, {shards} shard(s) -> max ~{(shards or 0) * 1000} records/s")
    print(f"{len(fleet.events)} live events across {len(PLANNERS)} planners, {len(fleet.sensors)} sensors, {args.rate} readings/s")
    print("Ctrl+C to stop\n")

    start = time.monotonic()
    last_report = start
    try:
        while True:
            tick_start = time.monotonic()
            elapsed = tick_start - start
            if elapsed >= args.duration or (args.total and sender.sent >= args.total):
                break
            records = fleet.tick(args.rate, tick_start)
            if args.total:
                records = records[: args.total - sender.sent]
            sender.send(records)

            now = time.monotonic()
            if now - last_report >= 5:
                print(f"t={elapsed:5.0f}s  sent={sender.sent:>9,}  avg={sender.sent / max(now - start, 1e-9):7.1f} rec/s  "
                      f"throttled-retries={sender.throttled:,}  failed={sender.failed:,}")
                last_report = now
            spare = 1.0 - (now - tick_start)
            if spare > 0:
                time.sleep(spare)
            elif spare < -0.5:
                print(f"  producer fell behind by {-spare:.1f}s (lower --rate or add shards)")
    except KeyboardInterrupt:
        print("\nstopped")

    took = time.monotonic() - start
    print(f"\ndone: {sender.sent:,} records in {took:.0f}s ({sender.sent / max(took, 1e-9):.1f} rec/s), "
          f"{sender.throttled:,} throttled retries, {sender.failed:,} failed")


if __name__ == "__main__":
    main()
