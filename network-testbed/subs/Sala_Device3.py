import asyncio
import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion

print(list(CallbackAPIVersion))

# --- CONFIGURATION ---
BROKER = 'localhost'
PORT = 1883
SUB_TOPIC = "55:22:57:12/publisher/tasks=Motion;Min_Frequency=12;Max_Latency=175;Accuracy=89;Energy=1;"
CLIENT_ID = 'asyncio_mqtt_client_3'

# We'll store a reference to the main loop
MAIN_LOOP = None

# We'll initially store None for the publish topic
just_value = None

# For controlling when to start publishing
start_publishing_event = asyncio.Event()

def on_connect(client, userdata, flags, rc):
    print(f"[MQTT] Connected with result code {rc}")
    client.subscribe(SUB_TOPIC)
    print(f"[MQTT] Subscribed to {SUB_TOPIC}")

def on_message(client, userdata, msg):
    """
    Called by Paho's network thread when a message is received.
    We parse the payload, set the global topic, 
    and schedule an event.set() on the asyncio loop if needed.
    """
    global just_value
    payload = msg.payload.decode()
    print(f"[MQTT] Received on {msg.topic}: {payload}")

    parts = payload.split("=", 1)
    if len(parts) == 2:
        raw_topic = parts[1].strip()
        just_value = raw_topic
        print(f"[on_message] just_value set to: {just_value}")
    else:
        print("Error: No '=' found in payload.")

    # Trigger publishing once, but on the asyncio loop thread
    if not start_publishing_event.is_set():
        print("[MQTT] Triggering publisher...")
        MAIN_LOOP.call_soon_threadsafe(start_publishing_event.set)

async def publish_when_triggered(client):
    # Wait for the event to be set before publishing
    await start_publishing_event.wait()
    print("[PUBLISHER] Event received. Starting publishing...")

    while True:
        if just_value:
            message = "Hello from the other side"
            client.publish(just_value, message)
            print(f"[MQTT] Published to {just_value}: {message}")
        else:
            print("[PUBLISHER] 'just_value' is empty. No topic to publish to.")
        await asyncio.sleep(5)

async def main():
    global MAIN_LOOP
    MAIN_LOOP = asyncio.get_running_loop()

    client = mqtt.Client(client_id=CLIENT_ID, callback_api_version=CallbackAPIVersion.VERSION1)
    client.on_connect = on_connect
    client.on_message = on_message

    # Connect client safely within asyncio
    await MAIN_LOOP.run_in_executor(None, client.connect, BROKER, PORT, 60)
    client.loop_start()

    try:
        await publish_when_triggered(client)
    except asyncio.CancelledError:
        print("[Main] Cancelled.")
    finally:
        client.loop_stop()
        client.disconnect()

# --- ENTRY POINT ---
if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Main] Interrupted by user.")
