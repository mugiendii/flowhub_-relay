#!/usr/bin/env python3
"""
hubctl — the operator side, and the SUBSCRIBING half of the MQTT test pair.

(Named hubctl, not operator.py: a module called `operator` on sys.path shadows
the standard library's, and `collections` imports from it — so the whole
interpreter fails to start. Found the hard way.)

It does what a dashboard does, in a terminal:

  * subscribes to oceo2/points and learns what the device is, from the device
  * subscribes to oceo2/status and prints telemetry, marking `null` as MISSING
  * can send a command, and refuses to send one for a point the device did not
    report as writable
  * can push a SET_POINTS / SET_RULES file

    tools/hubctl.py watch
    tools/hubctl.py set alarm_lamp 1
    tools/hubctl.py push --file config.json

The refusal is the part worth noticing. It has no built-in list of point names:
everything it knows comes from the capability report. Point it at a cold room
and it offers a cold room's controls.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time

import paho.mqtt.client as mqtt

log = logging.getLogger("operator")


class Operator:
    def __init__(self, args) -> None:
        self.args = args
        self.capability: dict | None = None
        self.writable: set[str] = set()
        self.units: dict[str, str] = {}
        self.saw_status = False
        self.acked: dict | None = None

        self.client = mqtt.Client(client_id=args.client_id)
        if args.password:
            self.client.username_pw_set(args.username, args.password)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    def on_connect(self, client, userdata, flags, rc):
        if rc != 0:
            log.error("connect failed rc=%s", rc)
            return
        client.subscribe(self.args.topic_points, qos=1)
        client.subscribe(self.args.topic_status, qos=1)

    def on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode(errors="replace"))
        except ValueError:
            log.warning("non-JSON on %s", msg.topic)
            return

        if msg.topic == self.args.topic_points:
            self.adopt(payload)
            return

        data_id = payload.get("id")
        if data_id == 3003:
            log.info("device confirms %s <- %s  (ioWrite ran; not proof the "
                     "relay moved)", payload.get("point"), payload.get("value"))
            return
        if data_id == 4001:
            self.acked = payload
            ok = payload.get("ok")
            log.info("%s %s%s", payload.get("cmd"), "accepted" if ok else "REFUSED",
                     "" if ok else f" ({payload.get('error')})")
            return

        self.saw_status = True
        self.render(payload)

    def adopt(self, payload: dict) -> None:
        points = payload.get("points", [])
        self.capability = payload
        self.writable = {p["id"] for p in points if p.get("rw") == "w"}
        self.units = {p["id"]: p.get("unit", "") for p in points}
        log.info("device reports %d points; writable: %s",
                 len(points), ", ".join(sorted(self.writable)) or "none")
        for p in points:
            log.info("   %-12s %-11s %-11s %s",
                     p.get("id"), p.get("kind"), p.get("src"),
                     f"[{p.get('unit')}]" if p.get("unit") else "")

    def render(self, payload: dict) -> None:
        parts = []
        for key, value in payload.items():
            if key == "id":
                continue
            if value is None:
                # The whole point of publishing null: an operator must see a
                # dead sensor as missing, not as a plausible zero.
                parts.append(f"{key}=MISSING")
            else:
                unit = self.units.get(key, "")
                parts.append(f"{key}={value}{unit}")
        log.info("status  %s", "  ".join(parts))

    # -- actions ----------------------------------------------------------
    def wait_for_capability(self, timeout: float) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.capability is not None:
                return True
            time.sleep(0.05)
        return False

    def send_command(self, point: str, value: int) -> int:
        if not self.wait_for_capability(self.args.timeout):
            log.error("no capability report within %.1fs - refusing to send a "
                      "command to a device that has not said what it drives",
                      self.args.timeout)
            return 2
        if point not in self.writable:
            log.error("%r is not writable on this device. It reports: %s",
                      point, ", ".join(sorted(self.writable)) or "nothing")
            return 3
        payload = json.dumps({"point": point, "value": value})
        self.client.publish(self.args.topic_cmd, payload, qos=1)
        log.info("sent %s", payload)
        return 0

    def push_config(self, path: str) -> int:
        with open(path, "r", encoding="utf-8") as fh:
            text = fh.read()
        sent = 0
        # The editor writes the two commands separated by a blank line, so this
        # accepts either one object or that pair.
        for chunk in [c for c in text.split("\n\n") if c.strip()]:
            payload = json.loads(chunk)
            self.client.publish(self.args.topic_cfg, json.dumps(payload), qos=1)
            log.info("pushed %s", payload.get("cmd", "?"))
            sent += 1
            time.sleep(0.2)
        return 0 if sent else 4

    def run(self) -> int:
        self.client.connect(self.args.host, self.args.port, keepalive=30)
        self.client.loop_start()
        try:
            if self.args.action == "watch":
                deadline = time.time() + self.args.seconds if self.args.seconds else None
                while deadline is None or time.time() < deadline:
                    time.sleep(0.1)
                return 0
            if self.args.action == "set":
                rc = self.send_command(self.args.point, self.args.value)
                time.sleep(0.6)  # let the id:3003 confirmation land
                return rc
            if self.args.action == "push":
                rc = self.push_config(self.args.file)
                time.sleep(1.0)
                return rc
            return 1
        finally:
            self.client.loop_stop()
            self.client.disconnect()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("action", choices=["watch", "set", "push"])
    ap.add_argument("point", nargs="?", help="for `set`: the point id")
    ap.add_argument("value", nargs="?", type=int, help="for `set`: 0-255")
    ap.add_argument("--file", help="for `push`: a JSON config file")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=18830)
    ap.add_argument("--username", default="")
    ap.add_argument("--password", default="")
    ap.add_argument("--client-id", default="operator-1")
    ap.add_argument("--topic-status", default="oceo2/status")
    ap.add_argument("--topic-points", default="oceo2/points")
    ap.add_argument("--topic-cmd", default="oceo2/cmd")
    ap.add_argument("--topic-cfg", default="oceo2/cfg")
    ap.add_argument("--seconds", type=float, default=0)
    ap.add_argument("--timeout", type=float, default=5.0)
    args = ap.parse_args()

    if args.action == "set" and (args.point is None or args.value is None):
        ap.error("set needs a point and a value")
    if args.action == "push" and not args.file:
        args.file = args.point
    if args.action == "push" and not args.file:
        ap.error("push needs --file")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s operator %(message)s")
    return Operator(args).run()


if __name__ == "__main__":
    sys.exit(main())
