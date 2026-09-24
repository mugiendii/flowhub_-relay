#!/usr/bin/env python3
"""
Logic tests for the relay, with no broker and no network.

The thing worth testing is the one thing this relay does differently from
../mqtt_relay_oceo: it learns what a device drives instead of being told at
build time. So: does it learn correctly, does it forget the previous device,
and does it refuse when it does not know?

    .venv/bin/python3 test_relay.py
"""
import asyncio, importlib.util, json, os, sys

os.environ["LEGACY_OUTPUT_POINTS"] = ""
spec = importlib.util.spec_from_file_location("relay", "relay.py")
relay = importlib.util.module_from_spec(spec)
spec.loader.exec_module(relay)

fails = 0
def check(ok, what):
    global fails
    if ok:
        print(f"  ok   {what}")
    else:
        fails += 1
        print(f"  FAIL {what}")

class FakeWS:
    def __init__(self): self.sent = []
    async def send(self, data): self.sent.append(json.loads(data))

def main():
    print("capability learning")
    check(relay.writable_points == set(), "nothing is writable before a capability report")

    relay.adopt_capability({"points": [
        {"i":0,"id":"room_temp","kind":"AnalogIn","rw":"r"},
        {"i":1,"id":"door_open","kind":"DigitalIn","rw":"r"},
        {"i":2,"id":"compressor","kind":"DigitalOut","rw":"w"},
        {"i":3,"id":"alarm_lamp","kind":"DigitalOut","rw":"w"}]})
    check(relay.writable_points == {"compressor", "alarm_lamp"}, "cold room outputs learned")
    check("room_temp" not in relay.writable_points, "an input is never writable")

    relay.adopt_capability({"points": [
        {"i":0,"id":"fuel_level","kind":"AnalogIn","rw":"r"},
        {"i":1,"id":"start_rly","kind":"DigitalOut","rw":"w"}]})
    check(relay.writable_points == {"start_rly"},
          "a different device replaces the previous point list, same relay")

    relay.adopt_capability({"points": [
        {"id":"valve_a","kind":"DigitalOut"}, {"id":"t1","kind":"AnalogIn"}]})
    check(relay.writable_points == {"valve_a"}, "falls back to kind when rw is absent")

    print("malformed reports")
    before = set(relay.writable_points)
    relay.adopt_capability({"nope": 1})
    check(relay.writable_points == before, "a report with no points array is ignored")
    relay.adopt_capability({"points": [None, {"no_id": 1}, {"id": "", "rw": "w"}]})
    check(relay.writable_points == set(), "malformed entries are skipped, not trusted")

    print("command gating")
    relay.db = None
    relay.mqtt_client.publish = lambda *a, **k: None

    async def run():
        relay.writable_points = set()
        ws = FakeWS()
        await relay.handle_command({"point": "alarm_lamp", "value": 1}, ws)
        check(bool(ws.sent) and ws.sent[-1]["type"] == "NAK", "refused with no capability report")
        check("capability report" in ws.sent[-1]["error"], "the NAK explains why")

        relay.writable_points = {"alarm_lamp"}
        ws = FakeWS()
        await relay.handle_command({"point": "room_temp", "value": 1}, ws)
        check(bool(ws.sent) and ws.sent[-1]["type"] == "NAK", "refused a command aimed at an input")

        ws = FakeWS()
        await relay.handle_command({"point": "alarm_lamp", "value": 1}, ws)
        check(not ws.sent, "a valid command is forwarded with no complaint")

        ws = FakeWS()
        await relay.handle_command({"point": "alarm_lamp", "value": "x"}, ws)
        check(bool(ws.sent) and ws.sent[-1]["type"] == "NAK", "a non-integer value is refused")

        print("config passthrough")
        ws = FakeWS()
        await relay.handle_config({"cmd": "SET_POINTS", "points": []}, ws)
        check(bool(ws.sent) and ws.sent[-1]["type"] == "FORWARDED", "SET_POINTS is forwarded")
        ws = FakeWS()
        await relay.handle_config({"cmd": "REBOOT"}, ws)
        check(bool(ws.sent) and ws.sent[-1]["type"] == "NAK", "an unlisted command is not forwarded")

    asyncio.run(run())
    print(f"\n{'all checks passed' if not fails else str(fails) + ' FAILURES'}")
    return 1 if fails else 0

if __name__ == "__main__":
    sys.exit(main())
