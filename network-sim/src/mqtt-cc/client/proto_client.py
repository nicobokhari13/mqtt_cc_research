import paho.mqtt.client as mqtt
import proto_db as db
from proto_utils import ProtoUtils
import sys
import csv
from proto_asyncio import run_async_client
from algo_utils import Devices
from datetime import datetime
import status_handler

# USERNAME = "prototype"
# PASSWORD = "adminproto"
# STATUS_TOPIC = "status/#"
# PUBLISH_TOPIC = "sensor/"
# SUBS_WILL_TOPIC = "subs/will"
# NEW_SUBS_TOPIC = "subs/add" 
# LAT_CHANGE_TOPIC = "subs/change"

# def on_connect(client, userdata, flags, rc):

#     print("Connected with result code "+str(rc))
#     if(rc == 5):
#         print("Authentication Error on Broker")
#         sys.exit()
         
# def on_message(client, userdata, msg):
#     topic = msg.topic
#     payload = msg.payload.decode()

#     # Print MQTT message to console
#     if mqtt.topic_matches_sub(STATUS_TOPIC, topic):
#         status.handle_status_msg(client, msg)
#     if mqtt.topic_matches_sub(SUBS_WILL_TOPIC, topic):
#         will.updateDB(payload)
#     if mqtt.topic_matches_sub(NEW_SUBS_TOPIC, topic):
#         mapAssignments = algo.generateAssignments()
#         algo.sendCommands(mapAssignments, client)
#         pass
#     if mqtt.topic_matches_sub(LAT_CHANGE_TOPIC, topic):
#         # the message payload holds the topic with the changed max_allowed_latency
#         # algo handler should still generateAssignemnts, must handle case where max allowed latency of topic changed
#         mapAssignments = algo.generateAssignments(changedTopic=payload)
#         algo.sendCommands(mapAssignments, client)
#         pass

    
# data in each row of devices.csv
    # exp_type, deviceMac, battery, energy_per_execution, freq_range, topic publishings
    # 1         2           3           4                   5           6  -> ...

    #It looks like this takes 6 args
def main():
    utils = ProtoUtils()
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    devicesFile = sys.argv[1]
    in_sim = sys.argv[2]
    restart_window = sys.argv[3] # this is 10
    energy_per_execution = sys.argv[4]
    comm_energy = sys.argv[5]
    threshold = sys.argv[6]
    # devicePath sim 900 0.3 3
    print(f"Device File = {devicesFile}")
    print(f"Sim Value = {in_sim}")
    
    database = db.Database()
    database.openDB()
    #database.createDeviceTable() sala
    database.createVEVDevicesTable() #sala
    database.createPublishTable()

    try:
        with open(devicesFile, 'r', newline='') as devfile:
            reader = csv.reader(devfile)
            rows = list(reader)
    except FileNotFoundError:
        print("File not found: ", devicesFile)
        sys.exit()

    # Loop through devices.csv
    # Updated row format: deviceMac, accuracy, tasks, maximum_freq, energy, topics...
    for row in rows:
        try:
            # Extract individual fields from the row
            mac = row[0]  # deviceMac
            accuracy = row[1]  # JSON object or text representation of accuracy
            tasks = row[2]  # List of tasks supported by the device
            maximum_freq = row[3]  # Maximum frequency in Hz
            energy = row[4]  # Energy expressed as a percentage remaining
            
            # Add the device to the database
            database.addDevice(
                MAC_ADDR=mac,
                BATTERY=None,  # Not relevant here, but kept for compatibility if required elsewhere
            )
            
            # Update the new devices table with accuracy, tasks, maximum frequency, and energy
            insertDeviceQuery = '''
                INSERT OR REPLACE INTO devices (deviceMac, accuracy, tasks, maximum_freq, energy) 
                VALUES (?, ?, ?, ?, ?)
            '''
            device_values = (mac, accuracy, tasks, maximum_freq, energy)
            database.execute_query_with_retry(query=insertDeviceQuery, values=device_values, requires_commit=True)

            # Log publisher metrics (optional)
            status_handler.logPublisherMetrics(
                time=current_time,
                mac=mac,
                battery="None",  # Battery no longer applies in this schema
                memory_util_perc="None",
                cpu_util_perc="None",
                cpu_temp="None"
            )
            
            # Add topics for the device
            topicList = row[5:]  # Topics start at column 5 onward
            for topic in topicList:
                print("Sala this is topic: ", topic)
                database.addDeviceTopicCapability(MAC_ADDR=mac, TOPIC=topic)

        except IndexError as e:
            print(f"Row has missing fields and cannot be processed: {row}")
            print(f"Error: {e}")
            continue

    # Close the database connection
    database.closeDB()
    print("Tuvo exito hasta ahora")
    # create it once
    devices = Devices()
    devices.addEnergyPerExecution(energy_per_execution)
    devices.addEnergyPerCommunicationExecution(energy=comm_energy)
    devices.addConcurrencyThreshold(threshold)
    utils._timeWindow = int(restart_window)
    print(utils._timeWindow)
    if in_sim != "testbed":
        utils._in_sim = True
        utils._exp_type = in_sim
    elif in_sim:
        utils._in_sim = False
        utils._exp_type = "testbed"
    else: 
        print("Error with determining experiment type, exiting now")
        sys.exit()
    print(f"in_sim {utils._in_sim}")
    run_async_client()

if __name__ == "__main__":
    main()
