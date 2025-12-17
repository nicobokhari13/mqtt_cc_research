from algo_utils import Processing_Unit, Devices
import json
from proto_utils import ProtoUtils
from proto_db import Database

def resetPublishingsAndDeviceExecutions():
    db = Database()
    db.openDB()
    db.resetPublishings()
    db.resetDeviceExecutions()
    db.resetDeviceConsumptions()
    db.closeDB()

def generateAssignments(changedTopic = None, subLeft = None):
    db = Database()
    db.openDB()
    bestMacNumTopics = 0
    if changedTopic:
        db.resetDevicesPublishingToTopic(changedTopic)
        # if there was a change in a topic's max_allowed_latency,
        # find all the devices that currently publish to the topic and set publishing = 0
        # then run algorithm exactly the same as it will be treated like it was just added to the DB
    elif subLeft:
        db.resetAllDevicesPublishing()
    publishers = Devices()

    bestMac = None
    Emin = None
    Einc = None
    Enew = None
    Eratio = None
    topicsWithNoPublishers = db.topicsWithNoPublishers() # list of tuples with (topic, max_allowed_latency,latency requirement)
    print(topicsWithNoPublishers)
    # get all topics where publish = 0 for all capable devices

    # for each topic with none publishing
    for task in topicsWithNoPublishers: 
        topic = task[0]
        freq = task[1]
        #print(f"Task: {task}")
        # get devices capable of publishing to the topic
        capableDevices = db.devicesCapableToPublish(topicName=topic) # list of tuples with (deviceMac, battery, executions,consumption)
        #Here we are going to get the devices/publishers info like the 
        for device in capableDevices:
            mac = device[0]
            battery = device[1]
            num_exec = device[2]
            consumption = device[3]
            # get device publishing info with query
            devicePublishings = db.devicePublishing(MAC_ADDR=mac) # list of tuples with (topic, max_allowed_latency)

            # create processing unit at key = mac
            # add device info as a Processing_Unit in Devices singleton
            if mac not in publishers._units.keys():
                publishers.addProcessingUnit(Processing_Unit(macAddr=mac, capacity=battery, executions = num_exec, consumption=consumption))

            # add publishings to the device with macAddr = mac, and set device frequencies
            # add current device publishing info to assignments (topics that the device currently publishes to)

            # if there are topics the device already publishes to
            if devicePublishings:
                # add them to the unit
                publishers._units[mac].addPublishings(devicePublishings)
                if changedTopic and topic == changedTopic:
                    # if there was a latency change to changedTopic, then you must recalculate devices' number of executions
                    publishers._units[mac].resetExecutions()
            
            Einc = publishers._units[mac].energyIncrease(freq)

            # the device's new energy level after addition of the topic
            Enew = publishers._units[mac].currentEnergy() + Einc
            # the device's new energy level divided by its available battery
            Eratio = Enew / publishers._units[mac]._battery
            # if the new energy level is less than the battery, and 
            # the new energy level's ratio to the battery is smaller than the min 
            if not Emin:
                bestMac = mac
                Emin = Eratio
                # number of keys in the assignments' dictionary is the number of topics assigned to that 
                bestMacNumTopics = len(publishers._units[bestMac]._assignments.keys())
            elif (Enew <= publishers._units[mac]._battery and Eratio < Emin and len(publishers._units[mac]._assignments.keys()) < bestMacNumTopics):
                bestMac = mac
                Emin = Eratio
                bestMacNumTopics = len(publishers._units[bestMac]._assignments.keys())
            
        if bestMac != None:
            # adding the assignment adds the task's frequency to the publishings variable
            
            publishers._units[bestMac].addAssignment(topic, freq)
            # executions changed
            publishers._units[bestMac].resetExecutions()

            # previous consumption
            print(f"device {bestMac} previous consumption = {publishers._units[bestMac]._consumption}")
            # recall Emin = Enew / battery -> Enew = Emin * battery
            newConsumption = Emin * publishers._units[bestMac]._battery
            publishers._units[bestMac].updateConsumption(newConsumption)
            print(f"device {bestMac} updated consumption = {publishers._units[bestMac]._consumption}")
            # update DB
            db.updateDeviceExecutions(MAC_ADDR=bestMac, NEW_EXECUTIONS=publishers._units[bestMac]._numExecutions)
            db.updateDeviceConsumptions(MAC_ADDR=bestMac, NEW_CONSUMPTIONS=publishers._units[bestMac]._consumption)

        bestMac = None
        Emin = None
        Einc = None
        Enew = None
        Eratio = None
        bestMacNumTopics = 0
        

    # by this point, all the devices in Devices have their list of assignments
    
    # for each device in Devices
        # if the assignments is not None:
            # assignmentString = json.dumps(device._assignments)
            # Devices.addAssignmentsToCommand(deviceMac = device._mac, taskList = assignmentString)
    for macAddress, device in publishers._units.items():
        if not device._assignments:
            device._assignments = {"None":"None"}
        assignmentString = json.dumps(device._assignments)
        print(f"assignment string = {assignmentString}")
        publishers.addAssignmentsToCommand(deviceMac=macAddress, taskList=assignmentString)
        db.updatePublishTableWithPublishingAssignments(MAC_ADDR=macAddress, TOPICS=device._assignments.keys()) 
        
    db.closeDB()
    publishers.resetUnits()
    print(f"GENERATE ASSIGNMENTS FINAL COMMAND = {publishers._generated_cmd}")
    # while the publishers' unit information is reset, the assignments are preserved in generated_cmd
    return publishers._generated_cmd

def getPublisherExecutions():
    db = Database()
    db.openDB()
    rows = db.getAllDeviceExecutions() # list of tuples (deviceMac, executions)
    db.closeDB()       
    return rows 

def getPublisherConsumptions():
    db = Database()
    db.openDB()
    rows = db.getAllDeviceConsumptions()
    db.closeDB()
    return rows


