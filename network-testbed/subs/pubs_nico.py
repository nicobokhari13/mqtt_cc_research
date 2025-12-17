import paho.mqtt.client as mqtt
from typing import Dict
import psutil
import subprocess
#import powerProcessing as pwr_reader

class PublisherUtils:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance
    
    # on Pi, grab mac address with terminal, not programmatically
    def __init__(self) -> None:
        # Other attributes/constants
        self._STATUS_TOPIC = "00:22:13:12/Temperature" # where IoT device sends status
        self._got_cmd = None # set to true and mqtt awaits self. to be set to False after msg is received
        self._end_round = None # 
        self._publishes = None
        self._current_executions = 0

    def setParameters(self,Mac_addr, start_battery, in_sim, energy_per_execution = None):
        # Set Attributes to Parameters
        #self._USERNAME = username
        #self._PASSWORD = password
        self._timeWindow = 60 # 1 minute = time window where dev sends status, waits for response on command 
        self._deviceMac = Mac_addr # mac address of IoT device 
        self._battery = float(start_battery)
        self._CMD_TOPIC = "s00:22:13:12/publisher/tasks=Temperature,Humidity;Min_Frequency=5,10;Max_Latency=190,185;Accuracy=24,12;Energy=3,12;"#sensor/cmd/" + self._deviceMac # where IoT receives command on where to publish
        self._IN_SIM = in_sim
        if energy_per_execution:
            self._ENERGY_PER_EXECUTION = float(energy_per_execution)
        else:
            self._ENERGY_PER_EXECUTION = 0


    def setPublishing(self, pub_cmd:dict):
        if "None" in pub_cmd.keys():
            self._publishes = {}
        else:
            self._publishes = pub_cmd

    def decreaseSimEnergy(self):
        energyUsed = self._current_executions * self._ENERGY_PER_EXECUTION
        if self._battery != 0 and self._battery > energyUsed: 
            self._battery = self._battery - energyUsed
            return True
        else: 
            self._battery = 0
            return False
        
    def getExperimentEnergy(self):
        #self._battery = pwr_reader.readVoltage() * pwr_reader.readCurrent() * 60 # Joules = Watt-Seconds
        #self._battery = pwr_reader.readCurrent() # Amps
        self

    def saveNewExecutions(self, newExecutions):
        self._current_executions = float(newExecutions)
    
    def get_cpu_utilization(self):
        return psutil.cpu_percent(interval=5)
    
    def get_memory_utilization(self):
        return psutil.virtual_memory().percent
    
    def get_cpu_temperature(self):
        # temps in celsius
        result = subprocess.run(["vcgencmd", "measure_temp"], capture_output=True, text=True)
        temperature = result.stdout.strip().split("=")[1]
        temperature = float(temperature[:-2])
        return temperature