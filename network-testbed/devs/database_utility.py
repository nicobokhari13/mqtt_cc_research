import sqlite3
import numpy as np
import hungarian_task_assignment as hung
import ast  # For safe string-to-list conversion
np.set_printoptions(precision=2, suppress=True, linewidth=np.inf)
DB_PATH = "/mnt/c/Users/sala_/OneDrive/Documents/MQTTRESEARCH/mqtt-cc-research/brokers/broker-PIE/mosquitto/db/piedatabase.db"
def fetch_subscribers():
    """Fetch all rows from the Publishers table."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM Subscribers")
    rows = cursor.fetchall()
    conn.close()
    dictionary_subscribers= {}
    for row in rows:                   #
        dictionary_subscribers[row[0]]=[row[1],row[3], row[3],row[4]] 
    return dictionary_subscribers

def filtering(publishers_dict):
    subs_dict = fetch_subscribers()
    tasks_needed = []
    for key1 in subs_dict,publishers_dict :
        num = 0
        if subs_dict[key1][num]== publishers_dict[key1][num]:
            tasks_needed.append(publishers_dict[j])

    return tasks_needed

DB_PATH = "/mnt/c/Users/sala_/OneDrive/Documents/MQTTRESEARCH/mqtt-cc-research/brokers/broker-PIE/mosquitto/db/piedatabase.db"

def get_task_energy_matrix():
    """Fetch task-energy matrix and a mapping of DeviceMac to tasks & energy consumption."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT DeviceMac, Tasks, Energy FROM Publishers")
    data = cursor.fetchall()
    conn.close()

    # Dictionary to store DeviceMac → (Tasks, Energy)
    device_task_mapping = {}
    # List for constructing the energy matrix
    energy_matrix = []
    task_list = set()  # Collect unique tasks to determine matrix column order

    # Process database rows
    for row in data:
        try:
            device_mac = row[0]
            tasks = ast.literal_eval(row[1])  # Convert string to list of tasks
            energy_values = ast.literal_eval(row[2])  # Convert string to list of energy values

            # Ensure the number of energy values matches the number of tasks
            if len(tasks) == len(energy_values):
                device_task_mapping[device_mac] = dict(zip(tasks, energy_values))
                task_list.update(tasks)  # Add tasks to the set
        except Exception as e:
            print(f"Error processing row {row}: {e}")
    # Convert set of tasks to sorted list to maintain column consistency
    sorted_tasks = sorted(task_list)
    tasks_needed = filtering(device_task_mapping)
    
    # Create the matrix: each row corresponds to a DeviceMac, each column to a task
    for i in tasks_needed:
        for key in device_task_mapping :
            if tasks_needed[i] == device_task_mapping[key]:
                continue
            else:
                del device_task_mapping


    for device_mac, task_energy_map in device_task_mapping.items():
                row_values = [task_energy_map.get(task, 450004500) for task in sorted_tasks]  # Fill missing tasks with 0.0
                energy_matrix.append(row_values)

    # Convert to a NumPy array for structured representation
    energy_matrix = np.array(energy_matrix, dtype=np.float64)

    return energy_matrix, device_task_mapping, sorted_tasks  # Returns matrix, mapping, and task order

if __name__ == "__main__":
    matrix, mapping, tasks = get_task_energy_matrix()
    hung.hungarian_algorithm(matrix)
    # print("Task-Energy Matrix:\n", matrix)
    # print("\nDeviceMac → Task Mapping:")
    # for mac, mapping_dict in mapping.items():
    #     print(mac, ":", mapping_dict)
    print("\nTask Order:", tasks)

