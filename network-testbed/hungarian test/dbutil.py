import sqlite3
import numpy as np
import hungarian_task_assignment as hung
import ast  
import time

np.set_printoptions(precision=2, suppress=True, linewidth=np.inf)

DB_PATH = "/mnt/c/Users/sala_/OneDrive/Documents/MQTTRESEARCH/mqtt-cc-research/brokers/broker-PIE/mosquitto/db/piedatabase.db"

def fetch_subscribers():
    """Return a dict keyed by subscriber MAC containing QoS constraints.

    {
        mac: {
            "topic_name": "90:85:78:33/subscriber",
            "tasks":        ["Motion", "Humidity"],
            "min_freq":     {"Motion": 2, "Humidity": 4},
            "max_latency":  {"Motion": 150, "Humidity": 300},
            "accuracy":     {"Motion": 95, "Humidity": 90},
        },
        ...
    }
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT s.DeviceMac,
               s.Tasks,
               s.Min_Frequency,
               s.MaxAllowedLatency,
               s.Accuracy,
               t.TopicName
        FROM Subscribers s
        JOIN Topics t ON s.DeviceMac = t.DeviceMac
    """
    )
    rows = cursor.fetchall()
    conn.close()

    subscribers_dict = {}
    for row in rows:
        device_mac  = row[0]
        tasks       = ast.literal_eval(row[1])
        min_freq    = ast.literal_eval(row[2])
        max_latency = ast.literal_eval(row[3])
        accuracy    = ast.literal_eval(row[4])
        topic_name  = row[5]

        subscribers_dict[device_mac] = {
            "topic_name":   topic_name,
            "tasks":       tasks,
            "min_freq":    dict(zip(tasks, min_freq)),
            "max_latency": dict(zip(tasks, max_latency)),
            "accuracy":    dict(zip(tasks, accuracy)),
        }

    return subscribers_dict


def filter_publishers(publishers_dict):
    """Drop any publisher‑task pair that cannot satisfy *any* subscriber’s constraints."""
    subscribers_dict = fetch_subscribers()

    # Aggregate the full set of tasks that are actually requested
    required_tasks: set[str] = set()
    for sub in subscribers_dict.values():
        required_tasks.update(sub["tasks"])

    filtered_publishers = {}
    for mac, task_data in publishers_dict.items():
        valid_tasks = {}
        for task, energy in task_data["energy"].items():
            if task not in required_tasks:
                continue
            for sub in subscribers_dict.values():
                if task not in sub["tasks"]:
                    continue
                meets_freq = (
                    task in task_data["max_freq"]
                    and task_data["max_freq"][task] >= sub["min_freq"][task]
                )
                meets_latency = (
                    task in task_data["max_latency"]
                    and task_data["max_latency"][task] <= sub["max_latency"][task]
                )
                meets_accuracy = (
                    task in task_data["accuracy"]
                    and task_data["accuracy"][task] >= sub["accuracy"][task]
                )
                if meets_freq and meets_latency and meets_accuracy:
                    valid_tasks[task] = energy
        if valid_tasks:
            filtered_publishers[mac] = valid_tasks

    return filtered_publishers, required_tasks


def fetch_publishers():
    """Return capability dict keyed by publisher MAC."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT p.DeviceMac,
               p.Tasks,
               p.Energy,
               p.Max_Frequency,
               p.Max_Latency,
               p.Accuracy,
               t.TopicName
        FROM Publishers p
        JOIN Topics t ON p.DeviceMac = t.DeviceMac
    """
    )
    data = cursor.fetchall()
    conn.close()

    device_task_mapping = {}
    for row in data:
        device_mac  = row[0]
        tasks       = ast.literal_eval(row[1])
        energy      = ast.literal_eval(row[2])
        max_freq    = ast.literal_eval(row[3])
        max_latency = ast.literal_eval(row[4])
        accuracy    = ast.literal_eval(row[5])
        topic_name  = row[6]

        if not (len({len(tasks), len(energy), len(max_freq), len(max_latency), len(accuracy)}) == 1):
            print(f"[Warning] Mismatch in array lengths for {device_mac}")
            continue

        device_task_mapping[device_mac] = {
            "topic_name":  topic_name,
            "energy":      dict(zip(tasks, energy)),
            "max_freq":    dict(zip(tasks, max_freq)),
            "max_latency": dict(zip(tasks, max_latency)),
            "accuracy":    dict(zip(tasks, accuracy)),
        }
    return device_task_mapping


def get_task_energy_matrix():
    device_task_mapping = fetch_publishers()
    filtered_publishers, task_set = filter_publishers(device_task_mapping)

    sorted_tasks = sorted(task_set)
    energy_matrix = [
        [task_energy_map.get(task, 9_999_999) for task in sorted_tasks]
        for task_energy_map in filtered_publishers.values()
    ]
    return (
        np.array(energy_matrix, dtype=float),
        filtered_publishers,
        sorted_tasks,
        device_task_mapping,
    )

def rel():
    matrix, filtered_publishers, tasks, device_task_map = get_task_energy_matrix()
    subscribers_dict = fetch_subscribers()

    # Reverse index: task -> list[subscriber_mac]
    task_to_subs = {}
    for sub_mac, info in subscribers_dict.items():
        for t in info["tasks"]:
            task_to_subs.setdefault(t, []).append(sub_mac)

    final_assignments: dict[str, list[tuple[str, str]]] = {}
    publisher_assignments: dict[str, list[str]] = {}

    if matrix.size == 0:
        return final_assignments, publisher_assignments

    assignments = hung.hungarian_algorithm(matrix)
    publisher_list = list(filtered_publishers.keys())

    for row_idx, col_idx in assignments:
        publisher_mac = publisher_list[row_idx]
        task          = tasks[col_idx]

        for sub_mac in task_to_subs.get(task, []):
            sub_topic = subscribers_dict[sub_mac]["topic_name"]
            final_assignments.setdefault(sub_topic, []).append((publisher_mac, task))

        publisher_topic = device_task_map[publisher_mac]["topic_name"]

        freq_required = max(
            subscribers_dict[sub]["min_freq"][task] for sub in task_to_subs.get(task, [])
        )

        publisher_assignments.setdefault(publisher_topic, []).append(f"{task}:{freq_required}")

    return final_assignments, publisher_assignments
