#!/usr/bin/env bash
set -e
mkdir -p logs

run() {
    local script=$1
    local base=$(basename "$script" .py)
    python3 -u "$script" 2>&1 | tee "logs/${base}.log" &
}

run subscriber.py
run subscriber2.py
run subscriber3.py
run subscriber4.py
run subscriber5.py
run subscriber6.py
run Sala_devicenew.py
run Sala_Devicenew2.py
run Sala_devicenew3.py
run Sala_devicenew4.py
run Sala_devicenew5.py
run Sala_devicenew6.py

echo "✅  All jobs started. Ctrl‑C to stop; tail -f logs/*.log to watch."
wait
