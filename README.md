# mqtt_relay_points

MQTT↔WebSocket relay for point-map firmware.

Separate instance from `../mqtt_relay_oceo` — same shape, same reasoning,
different topics (`oceo2/*`), different WebSocket port (`8767`, not `8766` or
`8765`). Not a modification of it: that relay is deployed and serving the
filtration skid.

```
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python3 relay.py
```

## What changed, and why it matters

`mqtt_relay_oceo` carries this:

```python
# Must match HMI_POINT_COUNT's 4 output points in the firmware's HmiPoints.cpp.
ALLOWED_POINTS = {"solenoid1", "solenoid2", "solenoid3", "pump1"}
```

That is the third place a water filter is hard-coded — after the firmware's
`HmiPoints.cpp` and the HMI's `ioDefaults.ts`. Running the same hardware as a
cold room meant editing and redeploying all three, and any one of them drifting
out of step meant commands silently going nowhere.

Here **the device says what it has.** It publishes a capability report on
`oceo2/points`; this relay caches it and validates commands against it. A cold
room's `alarm_lamp` and a generator's `start_rly` work with no change here.

```json
{"points":[
  {"i":0,"id":"room_temp","kind":"AnalogIn","src":"ads1115","unit":"degC","rw":"r"},
  {"i":3,"id":"alarm_lamp","kind":"DigitalOut","src":"mcu_pin","unit":"","rw":"w"}
]}
```

Publish it **retained**, so a relay restart does not have to wait for the next
report before it will accept a command.

### Before a capability report arrives

Commands are **refused**, and the browser gets a `NAK` saying why. That is
deliberate: with no capability report the relay does not know which points are
outputs, and guessing is how you energise something nobody asked for.

For a device still running the old firmware, name its outputs explicitly:

```
LEGACY_OUTPUT_POINTS=solenoid1,solenoid2,solenoid3,pump1
```

The relay logs loudly that it is running on that fallback.

## Testing it without hardware or a broker

`tools/` holds a publisher, a subscriber and a throwaway broker, so the whole
MQTT path can be exercised offline. **None of it touches the production
broker** — the test broker binds `127.0.0.1` on port 18830.

```
tools/test_mqtt.sh          # end-to-end, 21 checks
```

Or drive it by hand, in three terminals:

```
tools/test_broker.py                              # local broker, tests only
tools/fake_device.py --preset coldroom --fault room_temp
tools/hubctl.py watch
tools/hubctl.py set alarm_lamp 1
tools/hubctl.py push --file config.json
```

`fake_device.py` behaves like real firmware on the wire: retained capability
report, telemetry keyed by its own point names, `null` for a faulted point,
`id:3003` after applying a command, and a `SET_RULES` that refuses a version
which does not advance.

`hubctl.py` has **no built-in list of point names**. Everything it offers comes
from the capability report, so `tools/hubctl.py set solenoid1 1` against a cold
room is refused locally and never reaches the broker. Point it at a generator
and it offers a generator's controls. That refusal is the behaviour worth
watching for — it is the same reasoning this relay is built on.

(The subscriber is `hubctl.py`, not `operator.py`: a module named `operator` on
`sys.path` shadows the standard library's, which `collections` imports from, and
the interpreter fails to start at all.)

## Topics

| Topic | Direction | Carries |
|---|---|---|
| `oceo2/status` | device → relay | Periodic telemetry. Keys are the device's own point names. |
| `oceo2/points` | device → relay | Capability report. Retained. |
| `oceo2/cmd` | relay → device | `{"point":"alarm_lamp","value":1}` |
| `oceo2/cfg` | relay → device | `SET_POINTS` / `SET_RULES` / `SET_NET`, forwarded unchanged |

Configuration is forwarded **unvalidated** on purpose. The device validates it,
and re-implementing `points.cpp`'s rules here would only give them a second
place to drift out of step with the firmware.

## Event log

Every message is logged to SQLite (`SQLITE_PATH`, default `events.db`), tagged
with the firmware's `id`:

| `data_id` | Meaning |
|---|---|
| `1001` | Periodic full status |
| `2002` | A command from the browser |
| `2003` | A configuration pushed from the browser |
| `3003` | "Command consumed" — the device confirms `ioWrite()` ran |
| `4004` | A capability report |

```
sqlite3 events.db "SELECT datetime(ts,'unixepoch'), data_id, payload FROM events ORDER BY ts DESC LIMIT 20;"
sqlite3 events.db "SELECT payload FROM events WHERE data_id = 4004 ORDER BY ts DESC LIMIT 1;"
```

`3003` still means only that the firmware called `digitalWrite()`. Cross-check
it against a multimeter on the physical pin: if the row says HIGH and the meter
reads LOW, the fault is downstream of the MCU — driver board, relay, wiring.

## Secrets

`MQTT_PASSWORD` is intentionally blank in the service file. The sibling relay
has a live broker password committed in both `relay.py` and its unit file;
don't repeat that here. Set it with `systemctl edit mqtt_relay_points`, and
rotate the shared one when you get the chance.
