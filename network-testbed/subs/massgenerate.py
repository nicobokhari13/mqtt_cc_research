import sqlite3
import random

DB_PATH = "/mnt/c/Users/sala_/OneDrive/Documents/MQTTRESEARCH/mqtt-cc-research/brokers/broker-PIE/mosquitto/db/piedatabase.db"

TASKS_LIST = ["Temperature", "Humidity", "Motion", "Thermal Camera"]

def generate_mac():
    """Generate a random MAC address."""
    return ":".join(f"{random.randint(0, 255):02X}" for _ in range(6))

def generate_random_entry():
    """Generate a single random entry for the Publishers table with accurate mapping to tasks."""
    mac = generate_mac()
    num_tasks = random.randint(1, 3)  # Each publisher has 1 to 3 tasks
    tasks = random.sample(TASKS_LIST, num_tasks)

    # Generate corresponding values for each task
    max_freq = [random.randint(1, 10) for _ in range(num_tasks)]  # Example small frequency range
    max_latency = [random.randint(50, 200) for _ in range(num_tasks)]  # Realistic latency
    accuracy = [round(random.uniform(50, 100), 10) for _ in range(num_tasks)]  # High precision float
    energy = [random.choice([3.0, round(random.uniform(3, 20), 1)]) for _ in range(num_tasks)]  # Sometimes 0

    return (mac, str(tasks), str(max_freq), str(max_latency), str(accuracy), str(energy))

def insert_mass_data(num_entries=100):
    """Insert multiple random rows into the Publishers table, maintaining correct column structure."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for _ in range(num_entries):
        entry = generate_random_entry()
        cursor.execute("""
            INSERT INTO Publishers (DeviceMac, Tasks, Max_Frequency, Max_Latency, Accuracy, Energy) 
            VALUES (?, ?, ?, ?, ?, ?)
        """, entry)

    conn.commit()
    conn.close()
    print(f"Inserted {num_entries} rows into the Publishers table.")

if __name__ == "__main__":
    insert_mass_data(25)  # Generate 500 random entries
