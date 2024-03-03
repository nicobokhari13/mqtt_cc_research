import paho.mqtt.client as mqtt
import sys # command line parameters
import json # structure will & network latency msg


USERNAME = ""
PASSWORD = ""
# Read gpt on command line parameters
TEMP_TOPIC = "sensor/temperature"
LATENCY_QOS = "%latency%90"
WILL_TOPIC = "subs/will"
# The callback for when the client receives a CONNACK response from the broker.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    if(rc == 5):
        print("Authentication Error on Broker")
        exit()
    client.subscribe(TEMP_TOPIC + LATENCY_QOS)
    print("Subscribed to topic: " + TEMP_TOPIC)

# The callback for when a message is published to the broker, and the backendreceives it
def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode()

    # Print MQTT message to console
    if mqtt.topic_matches_sub(TEMP_TOPIC, topic):
        print(f"Topic: {topic}")
        print(f"Message: {payload}")

# Executed when script is ran

def main():
    if(len(sys.argv) == 1):
        print("Using default username and password for subscriber")
        USERNAME = "sub01"
        PASSWORD = "mqttcc01"
    elif(len(sys.argv) > 3):
        print(f"Error: Expected 3 command line arguments. Received {len(sys.argv)}")
        exit()
    else:
        USERNAME = sys.argv[1]
        PASSWORD = sys.argv[2]
    # create MQTT Client
    client = mqtt.Client()
    # Set Paho API functions to our defined functions
    client.on_connect = on_connect
    client.on_message = on_message
    # Set username and password 
    client.username_pw_set(username=USERNAME, password=PASSWORD)
    will_data = {
        "clientid":USERNAME, 
        "topics:": [TEMP_TOPIC + LATENCY_QOS]
        }
    will_payload = json.dumps(will_data)
    client.will_set(topic=WILL_TOPIC, payload=will_payload, qos=1)
    # Connect client to the Broker
    client.connect("localhost", 1883)

    # Run cliet forever
    while True:
        client.loop()

if __name__ == "__main__":
    main()