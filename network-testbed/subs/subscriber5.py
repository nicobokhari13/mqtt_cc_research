import csv
import json
import sys
import time
from pathlib import Path

import paho.mqtt.client as mqtt

BROKER_HOST = "localhost"
BROKER_PORT = 1883

DEVICE_MAC = "89:88:44:88"

CONTROL_TOPIC = (
    "90:85:78:33/subscriber/"
    "tasks=Motion;Max_Latency=200;Accuracy=98;Min_Frequency=6;Reliability=98;"
)

CSV_PATH = Path("power_data_5.csv")

START_TIME = time.perf_counter()
CSV_FILE = None
CSV_WRITER = None


def ensure_csv():
    global CSV_FILE, CSV_WRITER
    if CSV_FILE is None:
        CSV_FILE = CSV_PATH.open("a", newline="")
        CSV_WRITER = csv.writer(CSV_FILE)
        if CSV_FILE.tell() == 0:
            CSV_WRITER.writerow(["time_seconds", "power"])
        CSV_FILE.flush()


def log_power(val: float):
    ensure_csv()
    CSV_WRITER.writerow([f"{time.perf_counter() - START_TIME:.6f}", f"{val:.2f}"])
    CSV_FILE.flush()


def on_connect(client, userdata, flags, rc):
    if rc != 0:
        print(f"[SUB] connect error {rc}")
        sys.exit(1)
    print("[SUB] Connected – subscribing to control topic…")
    client.subscribe(CONTROL_TOPIC, qos=1)


def on_message(client, userdata, msg):
    topic = msg.topic
    payload_raw = msg.payload.decode("utf-8")

    print("\n[SUB] Incoming:")
    print("  Topic:", topic)
    print("  Payload:", payload_raw)

    # Control message MAC/TaskName
    if "/" in payload_raw and not payload_raw.lstrip().startswith("{"):
        mac, task = payload_raw.split("/", 1)
        data_topic = f"{mac}/{task}"
        client.subscribe(data_topic, qos=1)
        print(f"  → Subscribed to '{data_topic}'")
        return

    # Parse JSON data message
    try:
        data = json.loads(payload_raw)
    except json.JSONDecodeError:
        print("  ! non‑JSON payload – skip")
        return

    power_val = None
    for k in ("capacity", "voltage", "power"):
        if k in data:
            power_val = float(data[k])
            break

    if power_val is not None:
        log_power(power_val)
        print(f"  → Logged power {power_val:.2f}")
    else:
        print("  ! no power field found")

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
        print("[SUB] CSV closed – bye")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[SUB] interrupted")
