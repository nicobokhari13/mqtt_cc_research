#!/usr/bin/env python3
"""
Fast‑forward MQTT publisher with battery drain.

• Subscribes to a control topic.
• Control payload format (example):
      00:22:13:12/publisher/
      tasks=Temperature,Humidity;
      Min_Frequency=20,10;
      Max_Latency=190,185;Accuracy=24,12;Energy=3,12;
• The string after the first '=' (“Temperature,Humidity;…”) becomes the
  data‑publish topic.
• Min_Frequency numbers (Hz) drive PUBLISH_INTERVAL = 1 / max(freqs).
• Battery drain per cycle = E_real + P_duty * PUBLISH_INTERVAL
  (E_real from datasheet, P_duty = 2 W).
"""

import asyncio, json, re
import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
P_CONT_W = {
    "Temperature":   0.3,
    "Humidity":      0.2,
    "Motion":        0.6,
    "ThermalCamera": 0.8,
}
# ── MQTT CONFIG ────────────────────────────────────────────────────────
BROKER, PORT = "localhost", 1883
SUB_TOPIC = (
    "96:00:66:12/publisher/"
    "tasks=Temperature,Motion;"
    "Min_Frequency=11,10;"
    "Max_Latency=190,185;"
    "Accuracy=99,99;"
    "Energy=1.7,2.7;"
)
CLIENT_ID = "SixthClient"

# ── BATTERY / ENERGY MODEL ─────────────────────────────────────────────
ENERGY_CAPACITY_WH = 18.5
energy_remaining_wh = ENERGY_CAPACITY_WH   # mutable
# realistic per‑reading energies (joules)
E_REAL_J = {
    "Temperature": 0.00005,
    "Humidity":    0.00005,
    "Motion":      0.065,
    "ThermalCamera": 0.0167
}
P_DUTY_W = 2.0                             # extra “on” power per task

# globals set at runtime
PUBLISH_INTERVAL = 5                       # seconds (default until updated)
just_value = None                          # topic for data publishes
start_evt  = asyncio.Event()
loop_ref   = None

# ── helpers ────────────────────────────────────────────────────────────
def extract_publish_interval(ctrl_payload: str, fallback: float = 5.0) -> float:
    """
    Find 'Min_Frequency=f1,f2,...;' and return 1/max(f_i) seconds.
    Returns fallback if not found or malformed.
    """
    m = re.search(r"Min_Frequency=([^;]+)", ctrl_payload)
    if not m:
        return fallback
    try:
        freqs = [float(x) for x in m.group(1).split(",") if x]
        fastest = max(freqs)
        return 1.0 / fastest if fastest > 0 else fallback
    except ValueError:
        return fallback

def clean_task_tokens(raw_topic: str):
    """Strip ';key=value' fragments from each comma‑separated token."""
    return [tok.split(";", 1)[0] for tok in raw_topic.split(",") if tok]

# ── MQTT callbacks ─────────────────────────────────────────────────────
def on_connect(client, userdata, flags, rc):
    print(f"[MQTT] Connected ({rc}) – sub {SUB_TOPIC}")
    client.subscribe(SUB_TOPIC)

def on_message(client, userdata, msg):
    global TASK_FREQ, PUBLISH_INTERVAL, just_value

    payload = msg.payload.decode().strip()
    topic   = msg.topic                 # e.g. 00:66:13:12/publisher

    if payload.startswith("AssignedTasks="):
        # ---------------- NEW PROTOCOL ----------------
        tasks_part = payload.split("=", 1)[1]   # "Temp:20,Hum:10"
        TASK_FREQ = {}
        for item in tasks_part.split(","):
            if ":" in item:
                name, freq = item.split(":", 1)
                try:
                    TASK_FREQ[name.strip()] = float(freq)
                except ValueError:
                    continue
        if not TASK_FREQ:
            print("[CTRL] no valid task:freq pairs – ignored")
            return

        PUBLISH_INTERVAL = 1.0 / max(TASK_FREQ.values())
        just_value = f"{topic}/data"           # publish on "<mac>/data"
        print(f"[CTRL] tasks → {TASK_FREQ}")
        print(f"[CTRL] interval → {PUBLISH_INTERVAL:.3f}s")

        if not start_evt.is_set():
            loop_ref.call_soon_threadsafe(start_evt.set)
        return

    # -------------- legacy branch (optional) --------------
    # keep your old parsing here if you still support it


# ── async publisher loop ───────────────────────────────────────────────
async def publish_loop(client):
    global energy_remaining_wh
    await start_evt.wait()
    print("[PUB] trigger received – starting publish loop")

    while energy_remaining_wh > 0:
        # figure out tasks this cycle
        tasks = list(TASK_FREQ.keys())
        if not tasks:
            await asyncio.sleep(PUBLISH_INTERVAL)
            continue

        # energy for this cycle
        cycle_energy_j = 0.0
        for t in tasks:
            e_base = E_REAL_J.get(t, 0.0)
            p_cont = P_CONT_W.get(t, 0.1)           # default 0.1 W if not listed
            cycle_energy_j += e_base + p_cont * PUBLISH_INTERVAL
        energy_remaining_wh -= cycle_energy_j / 3600   #  ← NEW

        # publish status
        pct = max(0.0, round(energy_remaining_wh / ENERGY_CAPACITY_WH * 100, 2))
        payload = json.dumps({
            "tasks": tasks,
            "energy_remaining_percent": pct,
            "sim_time": asyncio.get_running_loop().time()
        })
        client.publish(just_value, payload, qos=1)
        print(f"[PUB] E={pct:6.2f}%  Δ={cycle_energy_j/3600:.4f} Wh "
              f"Int={PUBLISH_INTERVAL:.3f}s  → {just_value}")

        await asyncio.sleep(PUBLISH_INTERVAL)

    print("[PUB] Battery depleted – stopping publisher")
# ── entry point ────────────────────────────────────────────────────────
async def main():
    global loop_ref
    loop_ref = asyncio.get_running_loop()

    client = mqtt.Client(client_id=CLIENT_ID,
                         callback_api_version=CallbackAPIVersion.VERSION1)
    client.on_connect = on_connect
    client.on_message = on_message

    await loop_ref.run_in_executor(None, client.connect, BROKER, PORT, 60)
    client.loop_start()
    try:
        await publish_loop(client)
    finally:
        client.loop_stop(); client.disconnect()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Main] Interrupted")
