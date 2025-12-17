import sqlite3
import random

DB_NAME = "/mnt/c/Users/sala_/OneDrive/Documents/MQTTRESEARCH/mqtt-cc-research/brokers/broker-PIE/mosquitto/db/piedatabase.db"
def insert_example_rows():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # -- Example from the user for the Topics table --
    # 1) subscriber example row
    cursor.execute("""
        INSERT INTO Topics (DeviceMac, TopicName, Publishing)
        VALUES (?, ?, ?)
    """, (
        "89:33:44:44",
        "89:33:44:44/subscriber/tasks=Motion,Humidity;Max_Latency=290,335;Accuracy=0.9,0.8;Min_Frequency=5,10;",
        0
    ))

    # 2) publisher example row
    cursor.execute("""
        INSERT INTO Topics (DeviceMac, TopicName, Publishing)
        VALUES (?, ?, ?)
    """, (
        "00:22:13:12",
        "00:22:13:12/publisher/tasks=Humidity;Min_Frequency=10;Max_Latency=185;Accuracy=98;Energy=1;",
        1
    ))

    # 3) publisher device's direct task topic
    cursor.execute("""
        INSERT INTO Topics (DeviceMac, TopicName, Publishing)
        VALUES (?, ?, ?)
    """, (
        "00:22:13:12",
        "00:22:13:12/Humidity",
        0
    ))

    # -- Example from the user for the Subscribers table --
    cursor.execute("""
        INSERT INTO Subscribers (DeviceMac, Tasks, Min_Frequency, MaxAllowedLatency, Accuracy)
        VALUES (?, ?, ?, ?, ?)
    """, (
        "89:33:44:44",
        '["Motion","Humidity"]',
        '[5,10]',
        '[290,335]',
        '[86.56,94.32]'
    ))

    # -- Example from the user for the Publishers table --
    cursor.execute("""
        INSERT INTO Publishers (DeviceMac, Tasks, Max_Frequency, Max_Latency, Accuracy, Energy)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        "00:22:13:12",
        '["Humidity"]',
        '[10]',
        '[185]',
        '[98]',
        '[1]'
    ))

    conn.commit()
    conn.close()


# -----------------------------------------------------------------------------
# 3. Generate random data for 24 more Subscribers & Publishers
# -----------------------------------------------------------------------------

def random_mac_address():
    """Generate a random MAC address-like string."""
    return ":".join(f"{random.randint(0, 255):02x}" for _ in range(4))

def random_tasks(num_tasks=None):
    """Return a random subset of possible tasks."""
    all_possible_tasks = ["Motion", "Humidity", "Temperature", "Light", "Vibration"]
    if num_tasks is None:
        num_tasks = random.randint(1, 3)  # choose how many tasks to pick
    return random.sample(all_possible_tasks, num_tasks)

def random_float(low=0.0, high=100.0):
    """Generate a random float in [low, high]."""
    return round(random.uniform(low, high), 2)

def random_int(low=1, high=100):
    """Generate a random int in [low, high]."""
    return random.randint(low, high)

def insert_random_devices(count=24):
    """
    Insert 'count' random subscriber devices and 'count' random publisher devices,
    along with their Topics entries.
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for _ in range(count):
        # Generate a random subscriber
        sub_mac = random_mac_address()
        sub_tasks = random_tasks(random.randint(1,2))  # up to 2 tasks for subscribers
        sub_min_freqs = [random_int(1, 10) for __ in sub_tasks]
        sub_max_latency = [random_int(100, 500) for __ in sub_tasks]
        sub_accuracy = [random_float(80, 100) for __ in sub_tasks]

        # Insert into Subscribers table
        cursor.execute("""
            INSERT INTO Subscribers (
                DeviceMac, Tasks, Min_Frequency, MaxAllowedLatency, Accuracy
            ) VALUES (?, ?, ?, ?, ?)
        """, (
            sub_mac,
            str(sub_tasks), 
            str(sub_min_freqs),
            str(sub_max_latency),
            str(sub_accuracy)
        ))

        # Insert into Topics table to represent the subscriber configuration (like example)
        # e.g. sub_mac/subscriber/tasks=...,Max_Latency=...,Accuracy=...,Min_Frequency=...; => publishing=0
        # We'll join arrays into strings similarly to your example
        tasks_str = ",".join(sub_tasks)
        latencies_str = ",".join(str(x) for x in sub_max_latency)
        accuracies_str = ",".join(str(x) for x in sub_accuracy)
        min_freq_str = ",".join(str(x) for x in sub_min_freqs)

        subscriber_topic = (
            f"{sub_mac}/subscriber/"
            f"tasks={tasks_str};"
            f"Max_Latency={latencies_str};"
            f"Accuracy={accuracies_str};"
            f"Min_Frequency={min_freq_str};"
        )
        cursor.execute("""
            INSERT INTO Topics (DeviceMac, TopicName, Publishing) 
            VALUES (?, ?, ?)
        """, (sub_mac, subscriber_topic, 0))

        # Generate a random publisher
        pub_mac = random_mac_address()
        pub_tasks = random_tasks(random.randint(1,2))  # up to 2 tasks for publishers
        pub_max_freqs = [random_int(5, 20) for __ in pub_tasks]
        pub_max_latency = [random_int(100, 500) for __ in pub_tasks]
        pub_accuracy = [random_float(80, 100) for __ in pub_tasks]
        pub_energy = [random_int(1, 5) for __ in pub_tasks]

        # Insert into Publishers table
        cursor.execute("""
            INSERT INTO Publishers (
                DeviceMac, Tasks, Max_Frequency, Max_Latency, Accuracy, Energy
            ) VALUES (?, ?, ?, ?, ?, ?)
        """, (
            pub_mac,
            str(pub_tasks),
            str(pub_max_freqs),
            str(pub_max_latency),
            str(pub_accuracy),
            str(pub_energy)
        ))

        # Insert into Topics table to represent the publisher configuration (like example)
        tasks_str = ",".join(pub_tasks)
        max_freq_str = ",".join(str(x) for x in pub_max_freqs)
        latencies_str = ",".join(str(x) for x in pub_max_latency)
        accuracies_str = ",".join(str(x) for x in pub_accuracy)
        energy_str = ",".join(str(x) for x in pub_energy)

        publisher_topic = (
            f"{pub_mac}/publisher/"
            f"tasks={tasks_str};"
            f"Min_Frequency={max_freq_str};"
            f"Max_Latency={latencies_str};"
            f"Accuracy={accuracies_str};"
            f"Energy={energy_str};"
        )
        cursor.execute("""
            INSERT INTO Topics (DeviceMac, TopicName, Publishing) 
            VALUES (?, ?, ?)
        """, (pub_mac, publisher_topic, 1))

        # Additionally insert a row for the direct topic of each publisher’s tasks 
        # (like "00:22:13:12/Humidity" in your example).
        # For simplicity, we combine all tasks into a single topic, or do multiple rows if you like.
        for single_task in pub_tasks:
            direct_topic = f"{pub_mac}/{single_task}"
            cursor.execute("""
                INSERT INTO Topics (DeviceMac, TopicName, Publishing)
                VALUES (?, ?, ?)
            """, (pub_mac, direct_topic, 0))

    conn.commit()
    conn.close()


# -----------------------------------------------------------------------------
# Main Execution
# -----------------------------------------------------------------------------
if __name__ == "__main__":

    # 2. Insert your EXACT example rows
    insert_example_rows()

    # 3. Insert random devices (24 more, to make total = 25)
    insert_random_devices(count=24)

    print("Database population complete!")