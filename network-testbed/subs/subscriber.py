import csv
import json
import os
import sys
import time
from pathlib import Path

import paho.mqtt.client as mqtt
BROKER_HOST = "localhost"
BROKER_PORT = 1883

DEVICE_MAC = "89:33:44:44"  

CONTROL_TOPIC = (
    f"{DEVICE_MAC}/subscriber/"
    "tasks=Motion,Humidity;"
    "Max_Latency=290,335;"
    "Accuracy=0.9,0.8;"
    "Min_Frequency=5,10;"
)

# CSV output path
CSV_PATH = Path("power_log1.csv")
CSV_FILE = None
CSV_WRITER = None
SCRIPT_START = time.perf_counter()

def ensure_csv():
    """Open the CSV file (append) and write header if empty."""
    global CSV_FILE, CSV_WRITER
    if CSV_FILE is None:
        CSV_FILE = CSV_PATH.open("a", newline="")
        CSV_WRITER = csv.writer(CSV_FILE)
        # Write header if file was empty
        if CSV_FILE.tell() == 0:
            CSV_WRITER.writerow(["time_seconds", "power"])
        CSV_FILE.flush()


def log_power(value: float):
    """Log <elapsed time>, <power> to CSV."""
    ensure_csv()
    elapsed = time.perf_counter() - SCRIPT_START
    CSV_WRITER.writerow([f"{elapsed:.4f}", f"{value:.2f}"])
    CSV_FILE.flush()

def on_connect(client, userdata, flags, rc):
    if rc != 0:
        print(f"[SUB] Connection failed with code {rc}")
        sys.exit(1)

    print("[SUB] Connected – subscribing to control topic…")
    client.subscribe(CONTROL_TOPIC, qos=1)
    print(f"[SUB] Subscribed to {CONTROL_TOPIC}")


def on_message(client, userdata, msg):
    """Handle control‑messages **and** data messages.

    * Control messages look like "MAC/TaskName" → we subscribe to that topic.
    * Data messages are JSON, we print them and log <power> to CSV.
    """
    topic = msg.topic
    payload_raw = msg.payload.decode("utf-8")

    print("\n[SUB] Incoming:")
    print("  Topic:", topic)
    print("  Payload:", payload_raw)

    # ---------------- Control message (e.g. "AA:BB:CC/Temperature") ---------
    if "/" in payload_raw and not payload_raw.lstrip()[0] == "{":
        mac, task = payload_raw.split("/", 1)
        full_topic = f"{mac}/{task}"
        client.subscribe(full_topic, qos=1)
        print(f"  → Subscribed to data topic '{full_topic}'")
        return

    try:
        data = json.loads(payload_raw)
    except json.JSONDecodeError:
        print("  ! Payload is not valid JSON – skipping power log.")
        return

    # Extract a power‑like field (prefer 'power', then 'capacity', then 'voltage')
    power_val = None
    for key in ("power", "capacity", "voltage"):
        if key in data:
            power_val = float(data[key])
            break

    if power_val is not None:
        log_power(power_val)
        print(f"  → Logged power: {power_val:.2f}")
    else:
        print("  ! No power‑related field found – nothing logged.")


def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    print(f"[SUB] Connecting to {BROKER_HOST}:{BROKER_PORT} …")
    client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
    client.loop_forever()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[SUB] Interrupted – closing CSV …")
        if CSV_FILE:
            CSV_FILE.close()
