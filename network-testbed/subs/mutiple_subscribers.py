import paho.mqtt.client as mqtt
import sys
import time
import uuid
import re

BROKER_HOST = "localhost"
BROKER_PORT = 1883
NUM_SUBSCRIBERS = 25

def get_base_mac():
    """
    Retrieves the local machine's MAC address as a base (e.g. "AA:BB:CC:DD:EE").
    We'll append unique endings to generate distinct addresses for each client.
    """
    mac_int = uuid.getnode()  # 48-bit MAC as an integer
    mac_hex = f"{mac_int:012x}".upper()  # e.g. '001122AABBCC'
    # Split into pairs: ['00','11','22','AA','BB','CC']
    mac_pairs = re.findall('..', mac_hex)
    # We'll keep only the first 5 pairs for the "base", 
    # and let the 6th pair vary per subscriber index.
    base_mac_pairs = mac_pairs[:5]  # e.g. ['00','11','22','AA','BB']
    return ":".join(base_mac_pairs)  # e.g. "00:11:22:AA:BB"

def generate_device_mac(base_mac, index):
    """
    Generate a pseudo-unique MAC from the base MAC plus an index in the last octet.
    Example: base_mac="AA:BB:CC:DD:EE", index=5 -> "AA:BB:CC:DD:EE:05"
    """
    return f"{base_mac}:{index:02X}"

def on_connect(client, userdata, flags, rc):
    """
    Subscribes to "<device_mac>/subscriber/tasks=Temperature,Humidity;...".
    """
    if rc == 0:
        print(f"[SUBSCRIBER-{userdata['sub_id']}] Connected successfully.")
        device_mac = userdata["device_mac"]

        topic_for_qos = (
            f"{device_mac}/subscriber/"
            "tasks=Temperature,Humidity;"
            "Min_Frequency=5,10;"
            "Max_Latency=190,185;"
            "Accuracy=0.9,0.8;"
            
        )

        print(f"[SUBSCRIBER-{userdata['sub_id']}] Subscribing to topic: {topic_for_qos}")
        client.subscribe(topic_for_qos)
    else:
        print(f"[SUBSCRIBER-{userdata['sub_id']}] Connection failed with code {rc}")
        sys.exit(1)

def on_message(client, userdata, msg):
    """
    Called when a message is received.
    Prints topic/payload and transforms "MAC_TASK" -> "MAC/TASK" if found.
    """
    topic = msg.topic
    payload = msg.payload.decode("utf-8")

    # print(f"\n[SUBSCRIBER-{userdata['sub_id']}] Received a message!")
    # print(f"  Topic: {topic}")
    # print(f"  Raw Payload: {payload}")

    # Transform from "MAC_TASK" to "MAC/TASK"
    if "_" in payload:
        mac_part, task_part = payload.split("_", 1)
        transformed = f"{mac_part}/{task_part}"
        print(f"  Transformed Payload: {transformed}")
    else:
        print("  No transformation needed for payload.")

def create_subscriber_client(index, base_mac):
    """
    Creates one MQTT subscriber with a unique device MAC:
    AA:BB:CC:DD:EE:<index in hex>
    """
    # 1) Generate a unique device MAC
    device_mac = generate_device_mac(base_mac, index)
    
    # 2) Create a unique MQTT client ID
    client_id = f"SubscriberClient-{index}"
    
    # 3) Instantiate MQTT client
    client = mqtt.Client(client_id=client_id)
    
    # 4) Assign callbacks and store 'sub_id' and 'device_mac'
    userdata = {
        "sub_id": index,
        "device_mac": device_mac
    }
    client.user_data_set(userdata)
    client.on_connect = on_connect
    client.on_message = on_message
    
    return client

def main():
    base_mac = get_base_mac()
    print(f"[MAIN] Base MAC for this machine is: {base_mac}")
    print(f"[MAIN] Spinning up {NUM_SUBSCRIBERS} subscriber clients...\n")

    clients = []
    for i in range(NUM_SUBSCRIBERS):
        sub_client = create_subscriber_client(i, base_mac)
        
        # Connect
        sub_client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
        
        # Start a background thread for network events
        sub_client.loop_start()
        clients.append(sub_client)
        
        time.sleep(0.2)  # small delay so we don't spam the broker at once

    print(f"[MAIN] All {NUM_SUBSCRIBERS} subscribers created and connected.")
    print("[MAIN] Press Ctrl+C to exit.")

    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[MAIN] Disconnecting all subscriber clients...")

    for c in clients:
        c.loop_stop()
        c.disconnect()

    print("[MAIN] Exiting.")

if __name__ == "__main__":
    main()
