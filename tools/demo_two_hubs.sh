#!/usr/bin/env bash
# Two-hub demo: two Flow hubs, each with its own relay, both reachable from the
# Oceo HMI's Flow Hubs page.
#
# It exists to show the split you actually want:
#
#   DEVICE RULES    run on each hub. They keep working when the broker, the
#                   relay and the browser are all gone.
#   BROWSER RULES   run in the HMI, across hubs. They need the link, and they
#                   are the only place a cross-hub rule can live.
#
# Nothing here touches the production broker: the broker is the throwaway in
# test_broker.py on 127.0.0.1:18830.
#
#   tools/demo_two_hubs.sh          # start everything, leave it running
#   tools/demo_two_hubs.sh --scene  # start, then run the scripted scenario
set -uo pipefail
cd "$(dirname "$0")/.."
PY=.venv/bin/python3
PORT=18830
LOGS=$(mktemp -d)

cleanup() {
    echo
    echo "stopping..."
    kill ${PIDS[@]:-} 2>/dev/null
    wait 2>/dev/null
    rm -rf "$LOGS"
}
trap cleanup EXIT INT TERM
PIDS=()

start() { "$@" >"$LOGS/$1.log" 2>&1 & PIDS+=($!); }

echo "=============================================================="
echo "  Two-hub demo"
echo "=============================================================="
echo

# ---- broker -------------------------------------------------------------
$PY tools/test_broker.py --port $PORT --quiet >"$LOGS/broker.log" 2>&1 &
PIDS+=($!)
sleep 1
echo "broker         127.0.0.1:$PORT   (test only)"

# ---- hub A: filtration skid --------------------------------------------
# Local rule: pressure below 2 bar for 10 s stops the pump. Protective, so it
# has to be on the device -- it must work with the network down.
$PY tools/fake_device.py --preset filtration --port $PORT --interval 2 \
    --client-id hub-a --drift \
    --topic-status hubA/status --topic-points hubA/points \
    --topic-cmd hubA/cmd --topic-cfg hubA/cfg \
    --rule "pressure:lt:2:10:pump1:0" \
    --drive "34:pressure:1.0" \
    >"$LOGS/hubA.log" 2>&1 &
PIDS+=($!)

# ---- hub B: rooftop tank ------------------------------------------------
# Local rule: roof tank above 95% for 5 s shuts the inflow valve. Overflow
# interlock -- again, must not depend on a network.
$PY tools/fake_device.py --preset coldroom --port $PORT --interval 2 \
    --client-id hub-b --drift \
    --topic-status hubB/status --topic-points hubB/points \
    --topic-cmd hubB/cmd --topic-cfg hubB/cfg \
    --rule "room_temp:gt:8:5:alarm_lamp:1" \
    --drive "36:room_temp:12.0" \
    >"$LOGS/hubB.log" 2>&1 &
PIDS+=($!)
sleep 2

# ---- one relay per hub, because the HMI makes one WebSocket per hub ------
MQTT_HOST=127.0.0.1 MQTT_PORT=$PORT MQTT_PASSWORD= \
MQTT_TOPIC_STATUS=hubA/status MQTT_TOPIC_POINTS=hubA/points \
MQTT_TOPIC_CMD=hubA/cmd MQTT_TOPIC_CFG=hubA/cfg \
MQTT_CLIENT_ID=relay_a WS_PORT=8767 SQLITE_PATH="$LOGS/hubA.db" \
    $PY relay.py >"$LOGS/relayA.log" 2>&1 &
PIDS+=($!)

MQTT_HOST=127.0.0.1 MQTT_PORT=$PORT MQTT_PASSWORD= \
MQTT_TOPIC_STATUS=hubB/status MQTT_TOPIC_POINTS=hubB/points \
MQTT_TOPIC_CMD=hubB/cmd MQTT_TOPIC_CFG=hubB/cfg \
MQTT_CLIENT_ID=relay_b WS_PORT=8768 SQLITE_PATH="$LOGS/hubB.db" \
    $PY relay.py >"$LOGS/relayB.log" 2>&1 &
PIDS+=($!)
sleep 2

cat <<TXT

  HUB A  "Filtration Skid"     relay  ws://127.0.0.1:8767
         points  tds_in, pressure, tank_full, solenoid1, pump1
         DEVICE RULE  pressure < 2 bar for 10s  ->  pump1 off

  HUB B  "Cold Room"           relay  ws://127.0.0.1:8768
         points  room_temp, door_open, compressor, alarm_lamp
         DEVICE RULE  room_temp > 8 degC for 5s  ->  alarm_lamp on

  In the Oceo HMI, Flow Hubs page: create two hubs and connect each to the
  URL above. Link them to allow a cross-hub browser rule.

TXT

if [ "${1:-}" != "--scene" ]; then
    echo "  Running. Ctrl-C to stop."
    echo "  Logs: $LOGS"
    echo
    tail -f "$LOGS/hubA.log" "$LOGS/hubB.log" 2>/dev/null
    exit 0
fi

# ---------------------------------------------------------------- scenario
say() { printf '\n--- %s ---\n' "$1"; }
watch_for() {                     # watch_for <file> <pattern> <seconds> <label>
    local deadline=$((SECONDS + $3))
    while [ $SECONDS -lt $deadline ]; do
        grep -qF -- "$2" "$1" && { echo "  OK   $4"; return 0; }
        sleep 0.4
    done
    echo "  MISS $4"; return 1
}

say "1. Both hubs report themselves to their own relay"
$PY tools/hubctl.py watch --port $PORT --seconds 3 \
    --topic-status hubA/status --topic-points hubA/points >"$LOGS/wA.log" 2>&1
$PY tools/hubctl.py watch --port $PORT --seconds 3 \
    --topic-status hubB/status --topic-points hubB/points >"$LOGS/wB.log" 2>&1
grep -q "device reports 5 points" "$LOGS/wA.log" && echo "  OK   hub A reports 5 points" || echo "  MISS hub A"
grep -q "device reports 4 points" "$LOGS/wB.log" && echo "  OK   hub B reports 4 points" || echo "  MISS hub B"
grep -q "alarm_lamp" "$LOGS/wB.log" && echo "  OK   and its own point names, not hub A's" || echo "  MISS hub B names"

say "2. A command from the HMI reaches hub B, and only hub B"
$PY tools/hubctl.py set compressor 1 --port $PORT \
    --topic-status hubB/status --topic-points hubB/points --topic-cmd hubB/cmd \
    >"$LOGS/cmdB.log" 2>&1
grep -q "device confirms compressor" "$LOGS/cmdB.log" && echo "  OK   hub B applied it" || echo "  MISS hub B command"
grep -q "applied compressor" "$LOGS/hubA.log" && echo "  MISS leaked to hub A" || echo "  OK   hub A never saw it"

say "3. A command for hub A's points is refused at hub B"
$PY tools/hubctl.py set pump1 0 --port $PORT \
    --topic-status hubB/status --topic-points hubB/points --topic-cmd hubB/cmd \
    >"$LOGS/wrong.log" 2>&1
grep -q "not writable on this device" "$LOGS/wrong.log" \
    && echo "  OK   refused locally, never published" || echo "  MISS refusal"

say "4. THE POINT: cut the network, device rules carry on"
echo "  killing the broker and both relays..."
for p in "${PIDS[@]}"; do
    if ps -p "$p" -o args= 2>/dev/null | grep -qE "test_broker|relay.py"; then
        kill "$p" 2>/dev/null
    fi
done
sleep 1
echo "  broker and relays are gone. The hubs are now alone."
echo "  a physical change is scheduled on each hub while they are offline:"
echo "    hub A  pressure drops to 1.0 bar   (rule needs it held for 10s)"
echo "    hub B  room_temp rises to 12 degC  (rule needs it held for 5s)"
echo
watch_for "$LOGS/hubA.log" "PHYSICAL CHANGE" 40 "hub A saw the pressure drop, with no network"
watch_for "$LOGS/hubA.log" "RULE FIRED"      25 "hub A stopped its pump, with no network"
watch_for "$LOGS/hubB.log" "PHYSICAL CHANGE" 20 "hub B saw the temperature rise, with no network"
watch_for "$LOGS/hubB.log" "RULE FIRED"      20 "hub B raised its alarm, with no network"
echo
echo "  A cross-hub rule in the browser CANNOT do this. That is the split:"
echo "  protective logic on the device, cross-hub logic in the HMI."
echo
echo "Logs: $LOGS"
sleep 3
