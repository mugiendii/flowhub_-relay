#!/usr/bin/env python3
"""
MQTT <-> WebSocket relay for point-map firmware (Flow hub / Oceo Machine v2).

Separate instance from ../mqtt_relay_oceo -- same shape, same reasoning, but it
does not know what a solenoid is. That relay carries:

    ALLOWED_POINTS = {"solenoid1", "solenoid2", "solenoid3", "pump1"}
    # Must match HMI_POINT_COUNT's 4 output points in the firmware's HmiPoints.cpp.

...which is the third place a water filter is hard-coded, after the firmware's
HmiPoints.cpp and the HMI's ioDefaults.ts. Deploying the same hardware as a cold
room meant editing and redeploying all three.

Here the device says what it has. It publishes a capability report listing its
points and which are writable; this relay caches that and validates commands
against it. A cold room's "alarm_lamp" and a generator's "start_rly" work with
no change here.

Topics (all overridable by environment):
    oceo2/status  device -> broker   periodic telemetry, keys are the device's
                                     own point names
    oceo2/points  device -> broker   capability report (retained, so a relay
                                     restart does not have to wait for one)
    oceo2/cmd     broker -> device   {"point":"...","value":N}
    oceo2/cfg     broker -> device   SET_POINTS / SET_RULES / SET_NET, forwarded
                                     from the browser unchanged

Every message is logged to SQLite, tagged with the firmware's `id` field:
    1001 - periodic full status from the device
    2002 - a command sent from the browser
    2003 - a configuration pushed from the browser
    3003 - the device confirming it actually wrote a pin
    4004 - a capability report

Cross-reference 3003 rows against a multimeter on the physical pin: if the row
says HIGH but the meter reads LOW, the fault is downstream of the MCU
(driver/relay/wiring), not the firmware.

Config is via environment variables (see mqtt_relay_points.service).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import time

import paho.mqtt.client as mqtt
import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("relay")

MQTT_HOST = os.environ.get("MQTT_HOST", "157.173.107.18")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USERNAME = os.environ.get("MQTT_USERNAME", "admin")
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD", "")
MQTT_CLIENT_ID = os.environ.get("MQTT_CLIENT_ID", "points_web_relay")

MQTT_TOPIC_STATUS = os.environ.get("MQTT_TOPIC_STATUS", "oceo2/status")
MQTT_TOPIC_POINTS = os.environ.get("MQTT_TOPIC_POINTS", "oceo2/points")
MQTT_TOPIC_CMD = os.environ.get("MQTT_TOPIC_CMD", "oceo2/cmd")
MQTT_TOPIC_CFG = os.environ.get("MQTT_TOPIC_CFG", "oceo2/cfg")

WS_HOST = os.environ.get("WS_HOST", "127.0.0.1")
WS_PORT = int(os.environ.get("WS_PORT", "8767"))  # 8765 = pump relay, 8766 = oceo

SQLITE_PATH = os.environ.get("SQLITE_PATH", "events.db")

# If no status arrives for this long, tell the dashboard the device looks
# offline rather than showing stale numbers as if they were live.
STALE_AFTER_S = float(os.environ.get("STALE_AFTER_S", "15"))

# A traced payload is clipped so one oversized retained message cannot push a
# browser's log buffer out of the way. The browser is told when this happened
# rather than shown a silently shortened payload.
MAX_TRACE_CHARS = int(os.environ.get("MAX_TRACE_CHARS", "512"))

# Commands the browser may push straight through to the device. Everything else
# is dropped -- this relay forwards configuration, it does not invent it.
FORWARDABLE_COMMANDS = {"SET_POINTS", "SET_RULES", "SET_NET", "GET_NET", "GET_POINTS", "SAVE"}

# What to accept before any capability report has arrived. Empty is the safe
# default: with no capability report the relay does not know what is an output,
# and guessing is how you energise something by accident. Set
# LEGACY_OUTPUT_POINTS for a device still running HmiPoints.cpp firmware, e.g.
#   LEGACY_OUTPUT_POINTS=solenoid1,solenoid2,solenoid3,pump1
LEGACY_OUTPUT_POINTS = {
    p.strip() for p in os.environ.get("LEGACY_OUTPUT_POINTS", "").split(",") if p.strip()
}

CREATE_EVENTS_SQL = """
CREATE TABLE IF NOT EXISTS events (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    ts      REAL NOT NULL,
    data_id INTEGER NOT NULL,
    payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_ts_idx ON events (ts DESC);
CREATE INDEX IF NOT EXISTS events_data_id_idx ON events (data_id);
"""

clients: set = set()
latest_state: dict | None = None
capability: dict | None = None      # the device's own point list
writable_points: set[str] = set(LEGACY_OUTPUT_POINTS)
last_message_at: float = 0.0
loop: asyncio.AbstractEventLoop | None = None
db: sqlite3.Connection | None = None


async def log_event(data_id: int, payload: str) -> None:
    """Appends one row to the SQLite events log. Only ever called on the asyncio
    loop's own thread (directly from handle_client, or scheduled via
    run_coroutine_threadsafe from the paho MQTT thread) -- sqlite3 connections
    are not safe to share across threads otherwise."""
    if db is None:
        return
    try:
        db.execute(
            "INSERT INTO events (ts, data_id, payload) VALUES (?, ?, ?)",
            (time.time(), data_id, payload),
        )
    except Exception:
        log.exception("Failed to write event row")


def trace(direction: str, topic: str, payload: str) -> None:
    """Mirrors one MQTT message to every browser, as it appeared on the wire.

    Separate from the state broadcast on purpose. That one carries the relay's
    INTERPRETATION -- parsed, merged, with `online` and a timestamp bolted on.
    This carries the message, including the ones the interpretation throws
    away: a payload that would not parse, a 3003 command confirmation, a topic
    nothing handles. Those are exactly the messages worth seeing when the
    question is "is the device actually publishing?".
    """
    if loop is None or not clients:
        return
    asyncio.run_coroutine_threadsafe(
        broadcast({
            "type": "MQTT",
            "dir": direction,          # "in" = from the broker, "out" = published
            "topic": topic,
            "payload": payload[:MAX_TRACE_CHARS],
            "truncated": len(payload) > MAX_TRACE_CHARS,
            "ts": time.time(),
        }),
        loop,
    )


async def broadcast(message: dict) -> None:
    if not clients:
        return
    data = json.dumps(message)
    await asyncio.gather(*(c.send(data) for c in list(clients)), return_exceptions=True)


def adopt_capability(parsed: dict) -> None:
    """Learns the device's point list. This is the whole difference from the
    oceo relay: what is writable comes from the device, not from a constant."""
    global capability, writable_points
    points = parsed.get("points")
    if not isinstance(points, list):
        log.warning("Capability report has no points array, ignoring")
        return

    names, writable = [], set()
    for p in points:
        if not isinstance(p, dict):
            continue
        pid = p.get("id")
        if not isinstance(pid, str) or not pid:
            continue
        names.append(pid)
        # "rw":"w" is what points::describe() emits for an output. Fall back to
        # the kind so an older report still works.
        if p.get("rw") == "w" or str(p.get("kind", "")).endswith("Out"):
            writable.add(pid)

    capability = parsed
    writable_points = writable
    log.info("Capability: %d points, writable: %s", len(names), sorted(writable) or "none")


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        log.info("Connected to MQTT broker")
        for topic in (MQTT_TOPIC_STATUS, MQTT_TOPIC_POINTS):
            client.subscribe(topic)
            log.info("  subscribed to %s", topic)
    else:
        log.error("MQTT connect failed, rc=%s", rc)


def on_disconnect(client, userdata, rc):
    log.warning("MQTT disconnected, rc=%s (paho will auto-reconnect)", rc)


def on_message(client, userdata, msg):
    global latest_state, last_message_at
    raw = msg.payload.decode(errors="replace")
    # Traced BEFORE parsing, so a malformed payload still reaches the browser.
    # A payload that does not parse is the single most useful thing to see
    # here, and it is the one the old path dropped silently.
    trace("in", msg.topic, raw)
    try:
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            raise ValueError(f"expected a JSON object, got {type(parsed).__name__}")
    except (ValueError, json.JSONDecodeError) as exc:
        log.warning("Bad payload on %s: %s (%r)", msg.topic, exc, raw)
        return

    if msg.topic == MQTT_TOPIC_POINTS:
        adopt_capability(parsed)
        if loop is not None:
            asyncio.run_coroutine_threadsafe(log_event(4004, raw), loop)
            asyncio.run_coroutine_threadsafe(
                broadcast({"type": "CAPABILITY", **parsed}), loop
            )
        return

    data_id = parsed.get("id", 1001)  # unlabelled firmware -- treat as full status
    if loop is not None:
        asyncio.run_coroutine_threadsafe(log_event(data_id, raw), loop)

    if data_id == 3003:
        log.info("Command consumed: point=%s value=%s",
                 parsed.get("point", parsed.get("ch")), parsed.get("value"))
        return  # not a full status -- don't broadcast it as one

    state = {**parsed, "online": True, "timestamp": time.time()}
    latest_state = state
    last_message_at = time.time()
    if loop is not None:
        asyncio.run_coroutine_threadsafe(broadcast(state), loop)


async def stale_watchdog() -> None:
    """Flips the cached state's `online` flag (and re-broadcasts) if the device
    goes quiet for longer than STALE_AFTER_S."""
    global latest_state
    was_online = True
    while True:
        await asyncio.sleep(2)
        if latest_state is None:
            continue
        online = (time.time() - last_message_at) < STALE_AFTER_S
        if online != was_online:
            was_online = online
            latest_state = {**latest_state, "online": online}
            await broadcast(latest_state)


mqtt_client = mqtt.Client(client_id=MQTT_CLIENT_ID)
if MQTT_PASSWORD:
    mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
mqtt_client.on_connect = on_connect
mqtt_client.on_disconnect = on_disconnect
mqtt_client.on_message = on_message


async def handle_command(msg: dict, ws) -> None:
    """A single point command from the browser."""
    point = str(msg.get("point", ""))
    try:
        value = int(msg.get("value"))
    except (TypeError, ValueError):
        await ws.send(json.dumps({"type": "NAK", "error": "value must be an integer"}))
        return

    if not writable_points:
        # No capability report and no legacy list. Refusing beats guessing:
        # the relay genuinely does not know which points are outputs, and
        # picking wrong energises something nobody asked for.
        await ws.send(json.dumps({
            "type": "NAK", "point": point,
            "error": "no capability report yet - the device has not said what it drives",
        }))
        log.warning("Command for %r refused: no capability report", point)
        return

    if point not in writable_points:
        await ws.send(json.dumps({
            "type": "NAK", "point": point,
            "error": f"not a writable point on this device (have: {sorted(writable_points)})",
        }))
        log.warning("Command for unknown/non-output point: %r", point)
        return

    value = 0 if value <= 0 else (255 if value > 255 else value)
    payload = json.dumps({"point": point, "value": value})
    await log_event(2002, payload)
    log.info("Command -> %s: %s", MQTT_TOPIC_CMD, payload)
    mqtt_client.publish(MQTT_TOPIC_CMD, payload, qos=1)
    trace("out", MQTT_TOPIC_CMD, payload)


async def handle_config(msg: dict, ws) -> None:
    """A configuration push from the editor. Forwarded unchanged -- the device
    validates it, and this relay deliberately has no opinion about what a valid
    point map looks like. Duplicating that validation here would give it a
    second place to drift from the firmware."""
    cmd = str(msg.get("cmd", ""))
    if cmd not in FORWARDABLE_COMMANDS:
        await ws.send(json.dumps({"type": "NAK", "error": f"command not forwardable: {cmd}"}))
        return

    payload = json.dumps(msg)
    await log_event(2003, payload)
    log.info("Config -> %s: %s (%d bytes)", MQTT_TOPIC_CFG, cmd, len(payload))
    mqtt_client.publish(MQTT_TOPIC_CFG, payload, qos=1)
    trace("out", MQTT_TOPIC_CFG, payload)
    await ws.send(json.dumps({"type": "FORWARDED", "cmd": cmd}))


async def handle_client(websocket, _path=None):
    clients.add(websocket)
    log.info("Browser connected (%d total)", len(clients))
    try:
        # Send what we already know, so a page that just loaded is not blank
        # until the next publish interval.
        if capability is not None:
            await websocket.send(json.dumps({"type": "CAPABILITY", **capability}))
        if latest_state is not None:
            await websocket.send(json.dumps(latest_state))

        async for raw in websocket:
            try:
                msg = json.loads(raw)
                if not isinstance(msg, dict):
                    raise ValueError("expected an object")
            except (json.JSONDecodeError, ValueError):
                log.warning("Ignoring malformed message from browser: %r", raw[:200])
                continue

            if "cmd" in msg:
                await handle_config(msg, websocket)
            elif "point" in msg:
                await handle_command(msg, websocket)
            else:
                log.warning("Ignoring message with neither cmd nor point: %r", raw[:200])
    finally:
        clients.discard(websocket)
        log.info("Browser disconnected (%d total)", len(clients))


async def main() -> None:
    global loop, db
    loop = asyncio.get_running_loop()

    db = sqlite3.connect(SQLITE_PATH, isolation_level=None)  # autocommit
    db.executescript(CREATE_EVENTS_SQL)
    log.info("Logging events to %s", os.path.abspath(SQLITE_PATH))

    if LEGACY_OUTPUT_POINTS:
        log.warning("Using LEGACY_OUTPUT_POINTS until a capability report arrives: %s",
                    sorted(LEGACY_OUTPUT_POINTS))
    else:
        log.info("Commands refused until the device publishes a capability report on %s",
                 MQTT_TOPIC_POINTS)

    mqtt_client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=60)
    mqtt_client.loop_start()

    asyncio.create_task(stale_watchdog())

    async with websockets.serve(handle_client, WS_HOST, WS_PORT):
        log.info("WebSocket relay listening on ws://%s:%d", WS_HOST, WS_PORT)
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
