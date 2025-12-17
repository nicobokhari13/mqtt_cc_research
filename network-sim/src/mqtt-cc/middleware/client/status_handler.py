import paho.mqtt.client as mqtt
import proto_db as db
from proto_utils import ProtoUtils
import json
import csv
import psutil

utils = ProtoUtils()

def handle_status_msg(msg:str):
    database = db.Database()
    database.openDB()

    status_json = json.loads(msg)

    mac = status_json["deviceMac"]
    battery = status_json["battery"]
    time = status_json["time"]
    cpu_util_perc = getBrokerCpuUtil()
    cpu_temp = getBrokerCpuTemp()
    memory_util_perc= getBrokerMemoryUtil()
    # cpu_util_perc = status_json["cpu_utilization_percentage"]
    # cpu_temp = status_json["cpu_temperature"]
    # memory_util_perc= status_json["memory_utilization_percentage"]

    if utils._in_sim == "testbed":
        logTestBedMetrics(time, mac, battery)
    logPublisherMetrics(time, mac, battery, memory_util_perc, cpu_util_perc, cpu_temp)
    database.updateDeviceStatus(MAC_ADDR=mac, NEW_BATTERY=battery)
    database.closeDB()

def logPublisherMetrics(time, mac, battery, memory_util_perc, cpu_util_perc, cpu_temp):
    logFile = utils._logFile 
    data = [time, mac, battery, memory_util_perc, cpu_util_perc, cpu_temp]
    with open(logFile, 'a', newline='') as file:
        writer = csv.writer(file)
        
        # Append the data to the CSV file
        writer.writerow(data)
def logTestBedMetrics(time, mac, power_instant, remaining_power, memory_util_perc, cpu_util_perc, cpu_temp):
    logFile = utils._logFile_testbed
    data = [time, mac, power_instant, remaining_power, memory_util_perc, cpu_util_perc, cpu_temp]
    with open(logFile, 'a', newline='') as file:
        writer = csv.writer(file)

def getBrokerCpuUtil():
    return psutil.cpu_percent(interval=1)

def getBrokerCpuTemp():
    return psutil.sensors_temperatures()['coretemp'][0].current
    
    #return psutil.sensors_temperatures()

def getBrokerMemoryUtil():
    return psutil.virtual_memory().percent