import paho.mqtt.client as mqtt
import proto_db as db
import status_handler as status
# import will_topic_handler as will
import sys

USERNAME = "prototype"
PASSWORD = "adminproto"
STATUS_TOPIC = "status/#"
PUBLISH_TOPIC = "sensor/"
# SUBS_WILL_TOPIC = "subs/will"

# will msgs are no longer being used since the experiment will only use connected subscribers
    # must delete db before restarting system

SUBS_NET_LAT_TOPIC = "subs/netlat" # receive network lat from subs for some window of time

# TODO 3: Message handling for all topics in subscribers.txt (Large todo, to break up later)
# TODO 4: Algorithm to assign publish column in publish_select table based on # topics * battery

def on_connect(client, userdata, flags, rc):

    print("Connected with result code "+str(rc))
    if(rc == 5):
        print("Authentication Error on Broker")
        sys.exit()
         
def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode()

    # Print MQTT message to console
    if mqtt.topic_matches_sub(STATUS_TOPIC, topic):
        status.handle_status_msg(client, msg)
    # if mqtt.topic_matches_sub(SUBS_WILL_TOPIC, topic):
    #     print(f"Topic: {topic}")
    #     will.updateDB(payload)
    
# Executed when script is ran

def main():
    database = db.Database()
    database.openDB()
    database.createDeviceTable()
    database.createPublishSelectTable()
    database.closeDB()
    
    # Considering 3rd table be 
        # Device | Topic | Capability | Publishing
        # Devices are capable of publishing to many topics, and are publishing to at least 1

    # create MQTT Client
    client = mqtt.Client()
    # Set Paho API functions to our defined functions
    client.on_connect = on_connect
    client.on_message = on_message
    # Set username and password 
    client.username_pw_set(username=USERNAME, password=PASSWORD)
    # Connect client to the Broker
    client.connect("localhost", 1883)

    

    # Run cliet forever
    while True:
        client.loop()

if __name__ == "__main__":
    main()