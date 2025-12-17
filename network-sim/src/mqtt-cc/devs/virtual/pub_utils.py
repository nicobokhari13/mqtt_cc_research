import paho.mqtt.client as mqtt
from typing import Dict
import psutil
import subprocess

class PublisherUtils:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance
    
    # on Pi, grab mac address with terminal, not programmatically
    def __init__(self) -> None:
        # Other attributes/constants
        self._STATUS_TOPIC = "sensor/status" # where IoT device sends status sala where device sends fire detection
        self._got_cmd = None # set to true and mqtt awaits self. to be set to False after msg is received
        self._end_round = None # 
        self._publishes = None
        self._current_executions = 0
        self._previous_executions = 0

    def setParameters(self,Mac_addr, start_battery, in_sim, energy_per_execution, comm_energy):
        # Set Attributes to Parameters
        #self._USERNAME = username
        #self._PASSWORD = password
        self._timeWindow = 60 # 1 minute = time window where dev sends status, waits for response on command 
        self._deviceMac = Mac_addr # mac address of IoT device 
        self._battery = float(start_battery)
        self._CMD_TOPIC = "sensor/cmd/" + self._deviceMac # where IoT receives command on where to publish
        self._IN_SIM = in_sim
        self._ENERGY_PER_EXECUTION = float(energy_per_execution)
        self._COMM_ENERGY = float(comm_energy)
    #this is just where it derives where to publish to from the command. sala
    def setPublishing(self, pub_cmd:dict):
        if "None" in pub_cmd.keys():
            self._publishes = {}
        else:
            self._publishes = pub_cmd

    def decreaseSimEnergy(self):
        energyConsumption = self._consumption
        #energyUsed = self._current_executions * self._ENERGY_PER_EXECUTION
        if self._battery != 0 and self._battery > energyConsumption: 
            self._battery = self._battery - energyConsumption
            return True
        else: 
            self._battery = 0
            return False
        
    def getExperimentEnergy(self):
        self._battery = psutil.sensors_battery().percent

    def saveNewExecutions(self, newExecutions):
        # save the previous execution number
        self._previous_executions = self._current_executions
        self._current_executions = float(newExecutions)

    def saveConsumption(self, energy):
        self._consumption = float(energy)
    
    def getChangeInExecutions(self):
        return self._current_executions - self._previous_executions

    def get_cpu_utilization(self):
        return psutil.cpu_percent(interval=5)
    
    def get_memory_utilization(self):
        return psutil.virtual_memory().percent
    
    def get_cpu_temperature(self):
        result = subprocess.run(["vcgencmd", "measure_temp"], capture_output=True, text=True)
        temperature = result.stdout.strip().split("=")[1]
        return temperature