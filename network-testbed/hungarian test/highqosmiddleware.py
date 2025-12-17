import sqlite3
import ast
import json
import paho.mqtt.client as mqtt
from collections import defaultdict

DB_PATH = "/mnt/c/Users/sala_/OneDrive/Documents/MQTTRESEARCH/mqtt-cc-research/brokers/broker-PIE/mosquitto/db/piedatabase.db"

def fetch_subscribers():
    conn   = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT s.DeviceMac,
               s.Tasks,
               s.Min_Frequency,
               s.MaxAllowedLatency,
               s.Accuracy,
               s.Reliability,
               t.TopicName
          FROM Subscribers AS s
          JOIN Topics      AS t
            ON s.DeviceMac = t.DeviceMac
           AND t.Publishing = 0
    """)
    rows = cursor.fetchall()
    conn.close()

    subscribers = {}
    for device_mac, tasks_txt, minf_txt, maxl_txt, acc_txt, rel_txt, topic_name in rows:
        tasks       = ast.literal_eval(tasks_txt)
        min_freq    = ast.literal_eval(minf_txt)
        max_latency = ast.literal_eval(maxl_txt)
        accuracy    = ast.literal_eval(acc_txt)
        reliability = ast.literal_eval(rel_txt)

        subscribers[device_mac] = {
            "topic_name":  topic_name,
            "tasks":       tasks,
            "min_freq":    dict(zip(tasks,    min_freq)),
            "max_latency": dict(zip(tasks,    max_latency)),
            "accuracy":    dict(zip(tasks,    accuracy)),
            "reliability": dict(zip(tasks,    reliability)),
        }
    return subscribers

def fetch_publishers():
    conn   = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT p.DeviceMac,
               p.Tasks,
               p.Max_Frequency,
               p.Max_Latency,
               p.Accuracy,
               p.Energy,
               p.Reliability,
               t.TopicName
          FROM Publishers AS p
          JOIN Topics     AS t
            ON p.DeviceMac = t.DeviceMac
           AND t.Publishing = 1
    """)
    rows = cursor.fetchall()
    conn.close()

    publishers = {}
    for device_mac, tasks_txt, maxf_txt, maxl_txt, acc_txt, en_txt, rel_txt, topic_name in rows:
        tasks       = ast.literal_eval(tasks_txt)
        max_freq    = ast.literal_eval(maxf_txt)
        max_latency = ast.literal_eval(maxl_txt)
        accuracy    = ast.literal_eval(acc_txt)
        energy      = ast.literal_eval(en_txt)
        reliability = ast.literal_eval(rel_txt)

        publishers[device_mac] = {
            "topic_name":  topic_name,
            "tasks":       tasks,
            "max_freq":    dict(zip(tasks,    max_freq)),
            "max_latency": dict(zip(tasks,    max_latency)),
            "accuracy":    dict(zip(tasks,    accuracy)),
            "energy":      dict(zip(tasks,    energy)),
            "reliability": dict(zip(tasks,    reliability)),
        }
    return publishers

def assign_by_constraints(subs, pubs):
    """
    Returns a map:
      { sub_topic_name: [ "pub_mac/task", … ] }
    for every publisher‐task that meets:
      pub.max_freq    ≥ sub.min_freq
      pub.max_latency ≤ sub.max_latency
      pub.accuracy    ≥ sub.accuracy
      pub.reliability ≥ sub.reliability
    """
    assignments = defaultdict(list)

    for sub_mac, sub in subs.items():
        sub_topic = sub["topic_name"]
        for pub_mac, pub in pubs.items():
            for task in sub["tasks"]:
                if task not in pub["tasks"]:
                    continue

                if ( pub["max_freq"][task]    >= sub["min_freq"][task]
                 and pub["max_latency"][task] <= sub["max_latency"][task]
                 and pub["accuracy"][task]    >= sub["accuracy"][task]
                 and pub["reliability"][task] >= sub["reliability"][task]
                ):
                    assignments[sub_topic].append(f"{pub_mac}/{task}")
    return assignments

def on_connect(client, userdata, flags, rc):
    if rc != 0:
        print(f"[ERROR] MQTT connect failed (code {rc})")
        return

    print("[INFO] Connected — fetching DB and computing assignments…")
    subs = fetch_subscribers()
    pubs = fetch_publishers()
    assignments = assign_by_constraints(subs, pubs)

    # 1) Notify subscribers
    for sub_topic, payloads in assignments.items():
        for payload in payloads:
            client.publish(sub_topic, payload)
            print(f"[PUBLISH → SUB] Topic='{sub_topic}' Payload='{payload}'")

    # 2) Notify publishers with JSON {task, frequency}
    # Build reverse map sub_topic → sub_mac
    topic_to_submac = {info["topic_name"]: mac for mac, info in subs.items()}

    # Accumulate per‐publisher instructions
    pub_instructions = defaultdict(list)
    for sub_topic, payloads in assignments.items():
        sub_mac = topic_to_submac[sub_topic]
        for payload in payloads:
            pub_mac, task = payload.split("/", 1)
            freq = subs[sub_mac]["min_freq"][task]
            pub_instructions[pub_mac].append({"task": task, "frequency": freq})

    # Publish JSON to each publisher’s own topic
    for pub_mac, instr in pub_instructions.items():
        pub_topic = pubs[pub_mac]["topic_name"]
        msg = json.dumps({"AssignedTasks": instr})
        client.publish(pub_topic, msg)
        print(f"[PUBLISH → PUB] Topic='{pub_topic}' Payload={msg}")

if __name__ == "__main__":
    client = mqtt.Client()
    client.on_connect = on_connect

    # adjust host/port/auth as needed
    client.connect("localhost", 1883, keepalive=60)
    client.loop_forever()
