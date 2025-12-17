import csv
import json
import sys
import time
from pathlib import Path

import paho.mqtt.client as mqtt
BROKER_HOST = "localhost"
BROKER_PORT = 1883

DEVICE_MAC = "55:67:44:99"

CONTROL_TOPIC = (
    f"{DEVICE_MAC}/subscriber/"
    "tasks=Temperature,ThermalCamera;"
    "Max_Latency=240,300;Accuracy=88,98;Min_Frequency=5,.11;"
)

CSV_PATH = Path("power_log2.csv")

CSV_FILE = None
CSV_WRITER = None
START_TIME = time.perf_counter()

def ensure_csv():
    global CSV_FILE, CSV_WRITER
    if CSV_FILE is None:
        CSV_FILE = CSV_PATH.open("a", newline="")
        CSV_WRITER = csv.writer(CSV_FILE)
        if CSV_FILE.tell() == 0:
            CSV_WRITER.writerow(["time_seconds", "power"])
        CSV_FILE.flush()


def log_row(power_val: float):
    ensure_csv()
    elapsed = time.perf_counter() - START_TIME
    CSV_WRITER.writerow([f"{elapsed:.4f}", f"{power_val:.2f}"])
    CSV_FILE.flush()

def on_connect(client, userdata, flags, rc):
    if rc != 0:
        print(f"[SUB] Connection failed: {rc}")
        sys.exit(1)
    print("[SUB] Connected – subscribing to control topic…")
    client.subscribe(CONTROL_TOPIC, qos=1)
    print(f"[SUB] Subscribed to {CONTROL_TOPIC}")


def on_message(client, userdata, msg):
    topic = msg.topic
    payload_raw = msg.payload.decode("utf-8")

    print("\n[SUB] Incoming:")
    print("  Topic:", topic)
    print("  Payload:", payload_raw)

    # Control message MAC/TaskName → subscribe
    if "/" in payload_raw and not payload_raw.lstrip().startswith("{"):
        mac, task = payload_raw.split("/", 1)
        full_topic = f"{mac}/{task}"
        client.subscribe(full_topic, qos=1)
        print(f"  → Subscribed to '{full_topic}'")
        return

    # Data message JSON -> parse
    try:
        data = json.loads(payload_raw)
    except json.JSONDecodeError:
        print("  ! Not JSON – skip logging")
        return

    power_val = None
    for key in ("power", "capacity", "voltage"):
        if key in data:
            power_val = float(data[key])
            break

    if power_val is not None:
        log_row(power_val)
        print(f"  → Logged power {power_val:.2f}")
    else:
        print("  ! No power field found")

def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    print(f"[SUB] Connecting to {BROKER_HOST}:{BROKER_PORT} …")
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    try:
        client.loop_forever()
    finally:
        if CSV_FILE:
            CSV_FILE.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[SUB] Interrupted")
