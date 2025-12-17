import math
from typing import Dict
import copy
class Processing_Unit:
    _OBSERVATION_PERIOD = 60
    # same as publisher simulation start scripts (run_devXX.sh) 

    def __init__(self, macAddr:str, capacity:float, executions: float, consumption:float):
        self._mac = macAddr
        self._freqs = list()
        self._battery = capacity
        self._numExecutions = executions
        self._consumption  = consumption
        self._assignments = {}
        self._freq_min = None
 
    def currentEnergy(self):
        return self._consumption

    def updateConsumption(self, energyIncrease):
        self._consumption+=energyIncrease
    
    def resetMinimum(self):
        print(f"Contents of self._freqs: {self._freqs}")
        if self._freqs:
            self._freq_min = min(self._freqs)
        else:
            self._freq_min = None
    
    def calculateExecutions(self, newTask = None):
        #numExecutions = 0
        # threshold for execution interval MAY CHANGE
        # example: 28 <-> 30 <-> 32
        threshold = Devices._instance._CONCURRENCY_THRESHOLD
        # copy of freqs
        freqCopy = copy.deepcopy(self._freqs)

        # if there is a newTask, add it to the copy to simulate adding the task to the publisher
        if newTask:  
            # if there is a new task, add it to freqs, change the min
            freqCopy.append(newTask)
            #self._freq_min = min(freqCopy)


        if freqCopy: # if there are frequencies, then continue
            #freqCopy.remove(self._freq_min)
            print("don't remove min")
            pass
        else:# if you don't have any frequencies, then that means nothing is being added, so = 0 executions
            return 0
        # remove the min from the following calculations
        #freqCopy.remove(self._freq_min)
        frequency_multiples = ()
        same_execution_group = []
        group_min = None
        num_executions = 0 
        multiplier = 2

        if freqCopy:
            frequency_multiples = set(freqCopy)
            # check if there exists a number in the set of frequency_multiples where |multiplreOfFreq - freq| < threshold
            for freq in freqCopy: 
                multipleOfFreq = freq * multiplier
                while multipleOfFreq < self._OBSERVATION_PERIOD:
                    frequency_multiples.add(multipleOfFreq)
                    multiplier+=1
                    multipleOfFreq = freq * multiplier
                multiplier = 2
            # convert the multiples back into a list for sorting in order
            print(frequency_multiples, type(frequency_multiples))
            frequency_multiples = list(frequency_multiples)
            frequency_multiples.sort()
            print(frequency_multiples, type(frequency_multiples))

            # after inserting all all multiples, loop through again, find a group of elements that are within threshold
            for i in range(len(frequency_multiples)):
                if i == 0: # if this is the first element
                    # then add it to the execution group 
                    same_execution_group.append(frequency_multiples[i])
                    group_min = frequency_multiples[i]
                    print("added the first element")
                else:
                    if abs(frequency_multiples[i] - group_min) < threshold:
                        # if the absolute value between the frequency multiple and group min is less than threshold
                        # then add it to the same execution group
                        same_execution_group.append(frequency_multiples[i])
                        # the group_min shouldn't change 
                        print("element in the same execution")
                    else:
                        # if this element is not within the group_min's threshold, then increase the number of executions, 
                        num_executions+=1
                        same_execution_group.clear()
                        same_execution_group.append(frequency_multiples[i])
                        group_min = frequency_multiples[i]
                        print("element not in the same execution, resetting group")
                        # reset the same_execution_group, and add the current element
                        # increment the number of executions
                        # reset group min
                print("num execution = ", num_executions)
                print('====')
                print("index = ", i)
                print("group = ", same_execution_group)
            
            if len(same_execution_group):
                num_executions+=1
            print("num executions = ", num_executions)

            return num_executions

    def resetExecutions(self):
        self._numExecutions = self.calculateExecutions()
    
    def energyIncrease(self, task):
        # energy Increase = sensing executions (total) * energy + change in communication executiation * energy
        newExecutions = self.calculateExecutions(newTask = task)
        changeInExecutions = newExecutions - self._numExecutions
        print(f"change in executions: {changeInExecutions}")
        return (newExecutions * Devices._instance._ENERGY_PER_EXECUTION) + changeInExecutions * Devices._instance._COMM_ENERGY
    
    def addAssignment(self, topic:str, task, isNew = None):
        self._assignments[topic] = task
        self._freqs.append(task)
        self.resetMinimum()
        # add assignment to publisher
        print(self._assignments)
        # example: 
        # "sensor/temperature": 10 -> publish to sensor/temperature every 10 seconds

    def addPublishings(self, publishing_list:list):
        # publishing_set is a list of tuples (topic, max_allowed_latency)
        for publishing in publishing_list:
            print(publishing)
            self.addAssignment(topic = publishing[0], task = publishing[1])
            # add topic's frequency to device frequencies
        # after all frequencies added, reset frequency minimum 
        self.resetMinimum()
                

class Devices:
    _instance = None
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance
    
    def __init__(self) -> None:
        # key = macAddr, value = Processing_Unit
        self._units: Dict[str, Processing_Unit] = dict()
        self._generated_cmd: Dict[str, str] = dict() #sala 

    def addProcessingUnit(self, newUnit:Processing_Unit):
        self._units[newUnit._mac] = newUnit

    def resetUnits(self):
        self._units.clear()
    
    def addEnergyPerExecution(self, energy):
        self._ENERGY_PER_EXECUTION = float(energy)

    def addEnergyPerCommunicationExecution(self, energy):
        self._COMM_ENERGY = float(energy)

    def addConcurrencyThreshold(self, threshold):
        self._CONCURRENCY_THRESHOLD = int(threshold)
        

    def addAssignmentsToCommand(self, deviceMac:str, taskList:str):
        self._generated_cmd[deviceMac] = taskList
        # example
        # b8:27:eb:4f:15:95 : "{"sensor/temperature":10, "sensor/airquality": 34}"" Sala this is were the command format is.
        # mac address       :   topic they want to subscribe to and latency
    

    

        





