#!/usr/bin/env python3
"""
A Flow hub controller, simulated — the PUBLISHING half of the MQTT test pair.

It behaves like real firmware on the wire:

  * publishes its capability report to oceo2/points, RETAINED, on connect
  * publishes telemetry to oceo2/status every --interval seconds
  * subscribes to oceo2/cmd and drives the named point, then confirms with
    id:3003 — software-side confirmation only, exactly as the firmware's is
  * subscribes to oceo2/cfg and accepts SET_POINTS / SET_RULES, re-publishing
    its capability when the point map changes

Three presets, so the same process can be a filtration skid, a cold room or a
generator — which is the whole argument for a point map.

    tools/fake_device.py --preset coldroom --host 127.0.0.1 --port 18830

A faulted point publishes `null`, never 0, because that is what the firmware
does and a test that smoothed it over would be testing the wrong thing.
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import signal
import sys
import time

import paho.mqtt.client as mqtt

log = logging.getLogger("device")


class LocalRule:
    """
    A device-side rule, with the firmware's semantics rather than convenient
    ones. It exists to be demonstrated with the link cut: these keep running
    when the broker, the relay and the browser are all gone.

    Matches rule_engine.cpp on the three things that matter:
      * the persistence delay gates turning ON only - release is immediate, so
        a condition that clears does not hold an output for another delay;
      * a faulted sensor asserts the action at once, because the fault state is
        the rule's own action (a protective rule fails toward protecting);
      * one rule owns one actuator.

    Syntax:  sensor:op:threshold:hold_s:actuator:state
    e.g.     pressure:lt:2:10:pump1:0
    """

    OPS = {
        "lt": lambda v, t: v < t, "le": lambda v, t: v <= t,
        "gt": lambda v, t: v > t, "ge": lambda v, t: v >= t,
        "eq": lambda v, t: v == t, "ne": lambda v, t: v != t,
    }

    def __init__(self, spec: str):
        parts = spec.split(":")
        if len(parts) != 6:
            raise ValueError(f"rule needs 6 fields, got {len(parts)}: {spec!r}")
        self.sensor, op, threshold, hold, self.actuator, state = parts
        if op not in self.OPS:
            raise ValueError(f"unknown operator {op!r}; use {'/'.join(self.OPS)}")
        self.op = op
        self.threshold = float(threshold)
        self.hold_s = float(hold)
        self.state = int(state)
        self.since = None
        self.active = False
        self.spec = spec

    def describe(self) -> str:
        sym = {"lt": "<", "le": "<=", "gt": ">", "ge": ">=", "eq": "==", "ne": "!="}[self.op]
        return (f"{self.sensor} {sym} {self.threshold:g} for {self.hold_s:g}s "
                f"-> {self.actuator}={'on' if self.state else 'off'}")

    def evaluate(self, values, faulted, now) -> bool:
        """Returns True when the actuator's commanded state changed."""
        was = self.active

        if self.sensor in faulted:
            # Fail toward the rule's own action, immediately. Waiting out the
            # delay on a dead sensor is exactly what the delay is not for.
            self.active = True
            self.since = None
        else:
            v = values.get(self.sensor)
            if v is None:
                self.active = False
                self.since = None
            elif self.OPS[self.op](v, self.threshold):
                if self.since is None and not self.active:
                    self.since = now
                if self.since is not None and (now - self.since) >= self.hold_s:
                    self.active = True
                    self.since = None
            else:
                self.active = False      # release is immediate
                self.since = None

        return self.active != was

PRESETS = {
    "filtration": [
        {"id": "tds_in",    "kind": "AnalogIn",   "src": "ads1115", "unit": "",     "rw": "r", "lo": 0,   "hi": 2000},
        {"id": "pressure",  "kind": "AnalogIn",   "src": "ads1115", "unit": "bar",  "rw": "r", "lo": 0,   "hi": 16},
        {"id": "tank_full", "kind": "DigitalIn",  "src": "mcu_pin", "unit": "bool", "rw": "r"},
        {"id": "solenoid1", "kind": "DigitalOut", "src": "mcu_pin", "unit": "",     "rw": "w"},
        {"id": "pump1",     "kind": "DigitalOut", "src": "mcu_pin", "unit": "",     "rw": "w"},
    ],
    "coldroom": [
        {"id": "room_temp",  "kind": "AnalogIn",   "src": "ads1115", "unit": "degC", "rw": "r", "lo": -30, "hi": 40},
        {"id": "door_open",  "kind": "DigitalIn",  "src": "mcu_pin", "unit": "bool", "rw": "r"},
        {"id": "compressor", "kind": "DigitalOut", "src": "mcu_pin", "unit": "",     "rw": "w"},
        {"id": "alarm_lamp", "kind": "DigitalOut", "src": "mcu_pin", "unit": "",     "rw": "w"},
    ],
    "generator": [
        {"id": "fuel_level", "kind": "AnalogIn",   "src": "ads1115",    "unit": "%", "rw": "r", "lo": 0, "hi": 100},
        {"id": "batt_volts", "kind": "AnalogIn",   "src": "ads1115",    "unit": "V", "rw": "r", "lo": 0, "hi": 32},
        {"id": "gen_hours",  "kind": "AnalogIn",   "src": "modbus_reg", "unit": "",  "rw": "r", "lo": 0, "hi": 100000},
        {"id": "gen_run",    "kind": "DigitalIn",  "src": "mcu_pin",    "unit": "bool", "rw": "r"},
        {"id": "start_rly",  "kind": "DigitalOut", "src": "mcu_pin",    "unit": "",  "rw": "w"},
    ],
}


class FakeDevice:
    def __init__(self, args) -> None:
        self.args = args
        self.points = [dict(p) for p in PRESETS[args.preset]]
        self.values = {}
        self.faulted = set(args.fault.split(",")) - {""}
        self.pinned: set[str] = set()   # values a scheduled drive has fixed
        self.config_version = 0
        for p in self.points:
            self.values[p["id"]] = self._initial(p)

        # Scheduled physical changes: "at T seconds, this sensor reads X".
        # Not a command - the point is that these need no network, the way a
        # real tank draining needs no network. It is what makes the
        # network-down demonstration deterministic instead of waiting on a
        # random walk to wander across a threshold.
        self.drives = []
        for spec in args.drive:
            at, sensor, value = spec.split(":")
            self.drives.append([float(at), sensor, float(value), False])
        self.started = time.monotonic()

        self.rules = [LocalRule(spec) for spec in args.rule]
        for r in self.rules:
            log.info("local rule: %s", r.describe())

        self.client = mqtt.Client(client_id=args.client_id)
        if args.password:
            self.client.username_pw_set(args.username, args.password)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    @staticmethod
    def _initial(p):
        if p["kind"] == "AnalogIn":
            return round((p.get("lo", 0) + p.get("hi", 1)) / 2, 2)
        return 0

    # -- capability -------------------------------------------------------
    def capability(self) -> dict:
        return {
            "type": "CAPABILITY",
            "device": self.args.client_id,
            "schema": 1,
            "config_version": self.config_version,
            "points": [
                {"i": i, "id": p["id"], "kind": p["kind"], "src": p["src"],
                 "unit": p["unit"], "rw": p["rw"]}
                for i, p in enumerate(self.points)
            ],
        }

    def publish_capability(self) -> None:
        # Retained: a relay or dashboard that connects later must not have to
        # wait for the next publish to learn what this device is.
        self.client.publish(self.args.topic_points, json.dumps(self.capability()),
                            qos=1, retain=True)
        log.info("capability published (%d points, retained)", len(self.points))

    # -- telemetry --------------------------------------------------------
    def apply_scheduled_drives(self) -> None:
        elapsed = time.monotonic() - self.started
        for d in self.drives:
            at, sensor, value, done = d
            if done or elapsed < at:
                continue
            d[3] = True
            self.values[sensor] = value
            # Pin it, so the random walk cannot immediately undo the change
            # the demo depends on.
            self.faulted.discard(sensor)
            self.pinned.add(sensor)
            log.info("PHYSICAL CHANGE at t=%.0fs: %s is now %g", elapsed, sensor, value)

    def run_local_rules(self) -> None:
        """
        Device-side evaluation. Deliberately NOT gated on the MQTT connection:
        that is the whole point of a rule living on the device, and the demo
        turns on being able to cut the link and watch these carry on.
        """
        now = time.monotonic()
        for r in self.rules:
            if not r.evaluate(self.values, self.faulted, now):
                continue
            target = next((p for p in self.points if p["id"] == r.actuator), None)
            if target is None or target["rw"] != "w":
                log.warning("rule targets %r, which is not an output", r.actuator)
                continue
            self.values[r.actuator] = r.state if r.active else (0 if r.state else 1)
            log.info("RULE FIRED (local, no network needed): %s -> %s=%s",
                     r.describe(), r.actuator, self.values[r.actuator])

    def telemetry(self) -> dict:
        out = {"id": 1001}
        for p in self.points:
            pid = p["id"]
            if pid in self.faulted:
                # null, never 0. A dead probe reading 0 degC looks like a
                # working freezer.
                out[pid] = None
                continue
            v = self.values[pid]
            if p["kind"] == "AnalogIn" and self.args.drift and pid not in self.pinned:
                span = (p.get("hi", 1) - p.get("lo", 0)) * 0.01
                v = round(min(max(v + random.uniform(-span, span), p.get("lo", 0)),
                              p.get("hi", 1)), 2)
                self.values[pid] = v
            out[pid] = v
        return out

    # -- MQTT -------------------------------------------------------------
    def on_connect(self, client, userdata, flags, rc):
        if rc != 0:
            log.error("connect failed rc=%s", rc)
            return
        log.info("connected as %s", self.args.client_id)
        client.subscribe(self.args.topic_cmd, qos=1)
        client.subscribe(self.args.topic_cfg, qos=1)
        self.publish_capability()

    def on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode(errors="replace"))
        except ValueError:
            log.warning("ignoring non-JSON on %s", msg.topic)
            return

        if msg.topic == self.args.topic_cmd:
            self.apply_command(payload)
        elif msg.topic == self.args.topic_cfg:
            self.apply_config(payload)

    def apply_command(self, payload: dict) -> None:
        pid = payload.get("point")
        value = payload.get("value")
        target = next((p for p in self.points if p["id"] == pid), None)

        if target is None:
            log.warning("command for unknown point %r", pid)
            return
        if target["rw"] != "w":
            # The firmware refuses this too: a command may not be pointed at an
            # input channel.
            log.warning("refusing command for read-only point %r", pid)
            return
        try:
            value = max(0, min(255, int(value)))
        except (TypeError, ValueError):
            log.warning("bad value in command: %r", payload)
            return

        self.values[pid] = value
        log.info("applied %s <- %s", pid, value)
        # id:3003 confirms ioWrite() ran. It does NOT mean the relay moved.
        self.client.publish(self.args.topic_status,
                            json.dumps({"id": 3003, "point": pid, "value": value}), qos=1)

    def apply_config(self, payload: dict) -> None:
        cmd = payload.get("cmd")
        if cmd == "SET_POINTS":
            new_points = []
            for p in payload.get("points", []):
                kind = p.get("kind", "")
                new_points.append({
                    "id": p.get("id", ""), "kind": kind,
                    "src": str(p.get("source", "")).lower(),
                    "unit": p.get("unit", ""),
                    "rw": "w" if kind.endswith("Out") else "r",
                    "lo": p.get("valid_min", 0), "hi": p.get("valid_max", 1),
                })
            self.points = new_points
            self.values = {p["id"]: self._initial(p) for p in new_points}
            log.info("SET_POINTS accepted: %d points", len(new_points))
            self.client.publish(self.args.topic_status, json.dumps(
                {"id": 4001, "type": "ACK", "cmd": "SET_POINTS", "ok": True}), qos=1)
            self.publish_capability()

        elif cmd == "SET_RULES":
            version = payload.get("config_version", 0)
            if version <= self.config_version:
                # The firmware rejects a version that does not advance; a test
                # device that accepted one would hide that.
                log.warning("SET_RULES refused: version %s not newer than %s",
                            version, self.config_version)
                self.client.publish(self.args.topic_status, json.dumps(
                    {"id": 4001, "type": "ACK", "cmd": "SET_RULES", "ok": False,
                     "error": "not_newer"}), qos=1)
                return
            self.config_version = version
            log.info("SET_RULES accepted: %d rules, version %d",
                     len(payload.get("rules", [])), version)
            self.client.publish(self.args.topic_status, json.dumps(
                {"id": 4001, "type": "ACK", "cmd": "SET_RULES", "ok": True,
                 "config_version": version}), qos=1)
            self.publish_capability()
        else:
            log.warning("unknown config command %r", cmd)

    def run(self) -> None:
        self.client.connect(self.args.host, self.args.port, keepalive=30)
        self.client.loop_start()
        stop = {"now": False}
        signal.signal(signal.SIGINT, lambda *_: stop.update(now=True))
        signal.signal(signal.SIGTERM, lambda *_: stop.update(now=True))

        deadline = time.time() + self.args.seconds if self.args.seconds else None
        try:
            while not stop["now"]:
                # Rules first, so telemetry reports the state they just set.
                self.apply_scheduled_drives()
                self.run_local_rules()
                payload = json.dumps(self.telemetry())
                self.client.publish(self.args.topic_status, payload, qos=1)
                log.info("telemetry %s", payload)
                if deadline and time.time() >= deadline:
                    break
                time.sleep(self.args.interval)
        finally:
            self.client.loop_stop()
            self.client.disconnect()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--preset", choices=sorted(PRESETS), default="filtration")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=18830)
    ap.add_argument("--username", default="")
    ap.add_argument("--password", default="")
    ap.add_argument("--client-id", default="flowhub-sim-1")
    ap.add_argument("--topic-status", default="oceo2/status")
    ap.add_argument("--topic-points", default="oceo2/points")
    ap.add_argument("--topic-cmd", default="oceo2/cmd")
    ap.add_argument("--topic-cfg", default="oceo2/cfg")
    ap.add_argument("--interval", type=float, default=2.0)
    ap.add_argument("--seconds", type=float, default=0, help="stop after this long (0 = forever)")
    ap.add_argument("--drift", action="store_true", help="wander the analog values")
    ap.add_argument("--fault", default="", help="comma-separated point ids to publish as null")
    ap.add_argument("--drive", action="append", default=[],
                    help="scheduled physical change: seconds:sensor:value "
                         "(e.g. 25:pressure:1.0). Needs no network, by design.")
    ap.add_argument("--rule", action="append", default=[],
                    help="device-side rule: sensor:op:threshold:hold_s:actuator:state "
                         "(e.g. pressure:lt:2:10:pump1:0). Repeatable. These keep "
                         "running with the network down.")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s device  %(message)s")
    FakeDevice(args).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
