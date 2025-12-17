import asyncio
import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion

# --- GLOBALS ---
BROKER = 'localhost'
PORT = 1883
SUB_TOPIC = "00:22:13:12/publisher/tasks=Temperature,Humidity;Min_Frequency=20,10;Max_Latency=190,185;Accuracy=24,12;Energy=3,12;"
CLIENT_ID = 'asyncio_mqtt_client'

just_value = None
start_publishing_event = asyncio.Event()

# We'll store a reference to the main event loop so we can call_soon_threadsafe
MAIN_LOOP = None

def on_connect(client, userdata, flags, rc):
    print(f"[MQTT] Connected with result code {rc}")
    client.subscribe(SUB_TOPIC)
    print(f"[MQTT] Subscribed to {SUB_TOPIC}")

def on_message(client, userdata, msg):
    """
    Called from Paho’s network thread.
    We must schedule the asyncio Event to be set on the loop thread.
    """
    global just_value
    payload = msg.payload.decode("utf-8")
    print(f"[MQTT] Received on {msg.topic}: {payload}")

    parts = payload.split("=", 1)
    if len(parts) == 2:
        raw_topic = parts[1].strip()
        just_value = raw_topic.lstrip(",").strip()
        print(f"[on_message] just_value set to: {just_value}")
    else:
        print("Error: No '=' found in payload.")

    # Instead of calling start_publishing_event.set() directly,
    # schedule it on the event loop’s thread:
    if not start_publishing_event.is_set():
        print("[MQTT] Triggering publisher...")
        MAIN_LOOP.call_soon_threadsafe(start_publishing_event.set)

async def publish_when_triggered(client):
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
    MAIN_LOOP = asyncio.get_running_loop()  # store a reference to the loop

    client = mqtt.Client(client_id=CLIENT_ID, callback_api_version=CallbackAPIVersion.VERSION1)
    client.on_connect = on_connect
    client.on_message = on_message

    # Connect using a thread-safe executor call
    await MAIN_LOOP.run_in_executor(None, client.connect, BROKER, PORT, 60)
    client.loop_start()

    try:
        await publish_when_triggered(client)
    except asyncio.CancelledError:
        print("[Main] Cancelled.")
    finally:
        client.loop_stop()
        client.disconnect()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Main] Interrupted by user.")
