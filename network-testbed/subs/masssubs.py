import sqlite3
import random

DB_PATH = "/mnt/c/Users/sala_/OneDrive/Documents/MQTTRESEARCH/mqtt-cc-research/brokers/broker-PIE/mosquitto/db/piedatabase.db"

TASKS_LIST = ["Temperature", "Humidity", "Motion", "Thermal Camera"]

def generate_mac():
    """Generate a random MAC address."""
    return ":".join(f"{random.randint(0, 255):02X}" for _ in range(6))

def generate_random_entry():
    """Generate a single random entry for the Subscribers table."""
    mac = generate_mac()
    num_tasks = random.randint(1, 3)  # Each subscriber needs 1 to 3 tasks
    tasks = random.sample(TASKS_LIST, num_tasks)

    # Generate corresponding values for each task
    min_freq = [random.randint(1, 10) for _ in range(num_tasks)]  # Example frequency range
    max_latency = [random.randint(50, 500) for _ in range(num_tasks)]  # Example latency range
    accuracy = [round(random.uniform(50, 100), 2) for _ in range(num_tasks)]  # High precision accuracy

    return (mac, str(tasks), str(min_freq), str(max_latency), str(accuracy))

def insert_mass_data(num_entries=100):
    """Insert multiple random rows into the Subscribers table."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for _ in range(num_entries):
        entry = generate_random_entry()
        cursor.execute("""
            INSERT INTO Subscribers (DeviceMac, Tasks, Min_Frequency, MaxAllowedLatency, Accuracy) 
            VALUES (?, ?, ?, ?, ?)
        """, entry)

    conn.commit()
    conn.close()
    print(f"Inserted {num_entries} rows into the Subscribers table.")

if __name__ == "__main__":
    insert_mass_data(25)  # Generate 500 random entries