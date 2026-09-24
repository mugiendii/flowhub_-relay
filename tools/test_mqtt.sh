#!/usr/bin/env bash
# End-to-end MQTT test: broker + simulated device + operator, all local.
#
# Nothing here touches the production broker. The broker is the throwaway in
# tools/test_broker.py, bound to 127.0.0.1 on a high port.
set -uo pipefail
cd "$(dirname "$0")/.."
PY=.venv/bin/python3
PORT=18830
OUT=$(mktemp -d)
FAILURES=0

cleanup() { kill ${BROKER_PID:-} ${DEVICE_PID:-} 2>/dev/null; wait 2>/dev/null; rm -rf "$OUT"; }
trap cleanup EXIT

say()  { printf '\n=== %s ===\n' "$1"; }
pass() { printf '  ok   %s\n' "$1"; }
fail() { printf '  FAIL %s\n' "$1"; FAILURES=$((FAILURES+1)); }
want() { if grep -qF -- "$2" "$1"; then pass "$3"; else fail "$3"; fi; }
wantnot() { if grep -qF -- "$2" "$1"; then fail "$3"; else pass "$3"; fi; }

say "starting local broker on 127.0.0.1:$PORT"
$PY tools/test_broker.py --port $PORT --quiet >"$OUT/broker.log" 2>&1 &
BROKER_PID=$!
sleep 1

# ---------------------------------------------------------------- cold room
say "a cold room device"
$PY tools/fake_device.py --preset coldroom --port $PORT --interval 1 \
    --fault room_temp >"$OUT/device.log" 2>&1 &
DEVICE_PID=$!
sleep 2

$PY tools/hubctl.py watch --port $PORT --seconds 3 >"$OUT/watch.log" 2>&1
want "$OUT/watch.log" "device reports 4 points" "operator learns the point list from the device"
want "$OUT/watch.log" "writable: alarm_lamp, compressor" "it learns which points are writable"
want "$OUT/watch.log" "room_temp=MISSING" "an unplugged sensor shows as MISSING, not 0"
want "$OUT/watch.log" "door_open=0" "a real zero still reads as 0"

say "commanding a cold room output"
$PY tools/hubctl.py set alarm_lamp 1 --port $PORT >"$OUT/set_ok.log" 2>&1
want "$OUT/set_ok.log" '"point": "alarm_lamp", "value": 1' "the command is published"
want "$OUT/set_ok.log" "device confirms alarm_lamp" "the device confirms with id:3003"
want "$OUT/device.log" "applied alarm_lamp <- 1" "the device applied it"

say "commands the device never offered"
$PY tools/hubctl.py set room_temp 1 --port $PORT >"$OUT/set_input.log" 2>&1
rc=$?
want "$OUT/set_input.log" "is not writable on this device" "a command at an input is refused locally"
[ $rc -ne 0 ] && pass "and exits non-zero" || fail "and exits non-zero"

$PY tools/hubctl.py set solenoid1 1 --port $PORT >"$OUT/set_other.log" 2>&1
want "$OUT/set_other.log" "is not writable on this device" \
     "a point from another installation is refused"
wantnot "$OUT/device.log" "applied solenoid1" "and never reaches the device"

kill $DEVICE_PID 2>/dev/null; wait $DEVICE_PID 2>/dev/null; DEVICE_PID=

# --------------------------------------------------------------- generator
say "the same operator against a generator"
$PY tools/fake_device.py --preset generator --port $PORT --interval 1 \
    >"$OUT/device2.log" 2>&1 &
DEVICE_PID=$!
sleep 2

$PY tools/hubctl.py watch --port $PORT --seconds 3 >"$OUT/watch2.log" 2>&1
want "$OUT/watch2.log" "device reports 5 points" "it picks up a different point list"
want "$OUT/watch2.log" "start_rly" "with that installation's own output"
wantnot "$OUT/watch2.log" "alarm_lamp" "and no trace of the previous one"
want "$OUT/watch2.log" "modbus_reg" "including a Modbus-sourced point"

$PY tools/hubctl.py set start_rly 1 --port $PORT >"$OUT/set_gen.log" 2>&1
want "$OUT/set_gen.log" "device confirms start_rly" "commanding it works with no code change"

# ------------------------------------------------------------ retained cap
say "retained capability"
$PY tools/hubctl.py watch --port $PORT --seconds 2 --client-id late-joiner \
    >"$OUT/late.log" 2>&1
want "$OUT/late.log" "device reports 5 points" \
     "a client joining later still gets the capability report"

# --------------------------------------------------------------- config push
say "pushing a configuration"
cat > "$OUT/cfg.json" <<'JSON'
{"cmd":"SET_RULES","schema":1,"config_version":7,"rule_count":1,"rules":[
 {"enabled":true,"combiner":"Single",
  "conditions":[{"signal_index":0,"comparator":"lt","threshold":20}],
  "persistence_ms":60000,"action":"RaiseAlarm","output_id":4,"output_state":true}]}
JSON
$PY tools/hubctl.py push --file "$OUT/cfg.json" --port $PORT >"$OUT/push.log" 2>&1
want "$OUT/push.log" "SET_RULES accepted" "the device accepts a newer config version"

$PY tools/hubctl.py push --file "$OUT/cfg.json" --port $PORT >"$OUT/push2.log" 2>&1
want "$OUT/push2.log" "SET_RULES REFUSED" "replaying the same version is refused"
want "$OUT/push2.log" "not_newer" "and says why"

printf '\n---\n'
if [ "$FAILURES" -eq 0 ]; then
  echo "all MQTT checks passed"
else
  echo "$FAILURES MQTT checks FAILED"
fi
exit $FAILURES
