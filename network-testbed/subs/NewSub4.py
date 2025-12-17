#!/usr/bin/env python3
import csv, json, sys
from pathlib import Path
import paho.mqtt.client as mqtt

BROKER_HOST, BROKER_PORT = "localhost", 1883
DEVICE_MAC  = "30:31:32:33"

CONTROL_TOPIC = (
    f"{DEVICE_MAC}/subscriber/"
    "tasks=Temperature,Humidity;"
    "Max_Latency=280,340;"
    "Accuracy=0.85,0.80;"
    "Min_Frequency=7,12;"
)
CSV_PATH = Path("log_temp_hum.csv")

CSV_FILE = CSV_WRITER = None
def ensure_csv():
    global CSV_FILE, CSV_WRITER
    if CSV_FILE is None:
        CSV_FILE = CSV_PATH.open("a", newline=""); CSV_WRITER = csv.writer(CSV_FILE)
        if CSV_FILE.tell() == 0: CSV_WRITER.writerow(["sim_time_s", "energy_percent"])
def log_row(t, e): ensure_csv(); CSV_WRITER.writerow([f"{t:.2f}", f"{e:.2f}"])

def on_connect(c,*_): c.subscribe(CONTROL_TOPIC,1)
def on_message(c,_,m):
    p=m.payload.decode()
    if "/" in p and not p.strip().startswith("{"):
        mac,task=p.split("/",1); c.subscribe(f"{mac}/{task}",1); return
    try:d=json.loads(p)
    except: return
    if "sim_time" in d: log_row(d["sim_time"], d["energy_remaining_percent"])

cli=mqtt.Client(); cli.on_connect=on_connect; cli.on_message=on_message
cli.connect(BROKER_HOST,BROKER_PORT,60); cli.loop_forever()
