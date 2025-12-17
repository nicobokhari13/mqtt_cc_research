#!/usr/bin/env python3
import csv, json, sys
from pathlib import Path
import paho.mqtt.client as mqtt

# ── CONFIG ─────────────────────────────────────────────────────────────
BROKER_HOST, BROKER_PORT = "localhost", 1883
DEVICE_MAC  = "89:33:44:44"

CONTROL_TOPIC = (
    f"{DEVICE_MAC}/subscriber/"
    "tasks=Motion,Humidity;"
    "Max_Latency=290,335;"
    "Accuracy=90,90;"
    "Min_Frequency=5,10;"
)

# publisher’s control inbox
PUBLISHER_TOPIC = "00:77:13:12/publisher"

CSV_PATH   = Path("power_log1.csv")
CSV_FILE   = CSV_WRITER = None

# ── CSV helpers ────────────────────────────────────────────────────────
def ensure_csv():
    global CSV_FILE, CSV_WRITER
    if CSV_FILE is None:
        CSV_PATH.parent.mkdir(exist_ok=True)
        CSV_FILE   = CSV_PATH.open("a", newline="")
        CSV_WRITER = csv.writer(CSV_FILE)
        if CSV_FILE.tell() == 0:
            CSV_WRITER.writerow(["sim_time_s", "energy_percent"])

def log_row(t: float, pct: float):
    ensure_csv()
    CSV_WRITER.writerow([f"{t:.2f}", f"{pct:.2f}"])

# ── MQTT callbacks ─────────────────────────────────────────────────────
def on_connect(cli, *_):
    # 1️⃣ tell the publisher what tasks / frequencies we need
    cli.publish(PUBLISHER_TOPIC, CONTROL_TOPIC, qos=1)
    print(f"[SUB] sent control → {PUBLISHER_TOPIC}")

    # 2️⃣ listen for further control replies (task‑topic pairs, etc.)
    cli.subscribe(CONTROL_TOPIC, qos=1)
    print(f"[SUB] sub  {CONTROL_TOPIC}")

def on_message(cli, *_ , msg):
    payload = msg.payload.decode()
    # control message => subscribe to that data topic
    if "/" in payload and not payload.lstrip().startswith("{"):
        mac, task = payload.split("/", 1)
        cli.subscribe(f"{mac}/{task}", qos=1)
        return

    # data message
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return

    if "sim_time" in data and "energy_remaining_percent" in data:
        log_row(data["sim_time"], data["energy_remaining_percent"])
        print(f"[SUB] logged  t={data['sim_time']:.1f}s "
              f"E={data['energy_remaining_percent']:.2f}%")

# ── main ───────────────────────────────────────────────────────────────
def main():
    cli = mqtt.Client()
    cli.on_connect = on_connect
    cli.on_message = on_message

    cli.connect(BROKER_HOST, BROKER_PORT, 60)
    cli.loop_forever()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        if CSV_FILE:
            CSV_FILE.close()
