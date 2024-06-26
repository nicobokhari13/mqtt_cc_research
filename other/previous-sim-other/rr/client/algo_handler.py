from algo_utils import Processing_Unit, Devices
import json
from proto_utils import ProtoUtils
from proto_db import Database

utils = ProtoUtils()

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
    topicsWithNoPublishers = db.topicsWithNoPublishers() # list of tuples with (topic, max_allowed_latency)
    print(topicsWithNoPublishers)
    # get all topics where publish = 0 for all capable devices

    # for each topic with none publishing
    for task in topicsWithNoPublishers: 
        for mac, device in publishers._units.items():
            print(f"mac: {mac}, device {device._assignments}")
            print("------")
        topic = task[0]
        freq = task[1]
        print(f"Task: {task}")
        # get devices capable of publishing to the topic
        capableDevices = db.devicesCapableToPublish(topicName=topic) # list of tuples with (deviceMac, battery, executions)

        # for each device
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
                publishers.addProcessingUnit(Processing_Unit(macAddr=mac, capacity=battery, executions=num_exec))

            # add publishings to the device with macAddr = mac, and set device frequencies
            # add current device publishing info to assignments (topics that the device currently publishes to)

            # if there are topi cs the device already publishes to
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
            Eratio = Enew / device._battery
            # if the new energy level is less than the battery, and 
            # the new energy level's ratio to the battery is smaller than the min 
            if not Emin:
                bestMac = mac
                Emin = Eratio
            elif (Enew <= publishers._units[mac]._battery and Eratio < Emin):
                bestMac = mac
                Emin = Eratio
            
        if bestMac != None:
            # adding the assignment adds the task's frequency to the publishings variable
            print(publishers._units[bestMac]._mac)
            print(publishers._units[bestMac]._assignments)
            publishers._units[bestMac].addAssignment(topic, freq)
            print(publishers._units[bestMac]._assignments)

            # we know bestMac uses Emin energy, so reverse operations to get Einc
            Einc = (Emin * publishers._units[bestMac]._battery) - publishers._units[bestMac].currentEnergy()
            changeInExecutions = Einc / Processing_Unit._ENERGY_PER_EXECUTION
            print(f"{bestMac} used to execute at {publishers._units[bestMac]._numExecutions}")
            New_Executions = changeInExecutions + publishers._units[bestMac]._numExecutions
            print(f"{bestMac} now executes at {New_Executions}")
            publishers._units[bestMac]._numExecutions = New_Executions
            # update num executions in DB
            db.updateDeviceExecutions(MAC_ADDR=bestMac, NEW_EXECUTIONS=New_Executions)

        bestMac = None
        Emin = None
        Einc = None
        Enew = None
        Eratio = None
        

    # by this point, all the devices in Devices have their list of assignments
    
    # for each device in Devices
        # if the assignments is not None:
            # assignmentString = json.dumps(device._assignments)
            # Devices.addAssignmentsToCommand(deviceMac = device._mac, taskList = assignmentString)
    for macAddress, device in publishers._units.items():
        if device._assignments:
            assignmentString = json.dumps(device._assignments)
            print(f"assignment string = {assignmentString}")
            publishers.addAssignmentsToCommand(deviceMac=macAddress, taskList=assignmentString)
            db.updatePublishTableWithPublishingAssignments(MAC_ADDR=macAddress, TOPICS=device._assignments.keys()) 
    db.closeDB()
    publishers.resetUnits()
    print(f"generated final command = {publishers._generated_cmd}")
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

def roundRobinGeneration():
    db = Database()
    db.openDB()
    db.resetAllDevicesPublishing()
    publishers = Devices()
    tasks = db.topicsWithNoPublishers()
    for task in tasks:
        topic = task[0]
        freq = task[1]

        capableDevices = db.getCapableDevicesAlphabetically(topicName=topic) # list of tuples with (deviceMac, battery, executions)
        
        if (topic not in utils._topic_device_indices.keys()) or (utils._topic_device_indices[topic] + 1) >= len(capableDevices):
            utils._topic_device_indices[topic] = 0
        else: 
            utils._topic_device_indices[topic] += 1

        # for each device
        for i in range(len(capableDevices)):
            mac = capableDevices[i][0]
            battery = capableDevices[i][1]
            num_exec = capableDevices[i][2]
            consumption = capableDevices[i][3]
            if mac not in publishers._units.keys():
                publishers.addProcessingUnit(Processing_Unit(macAddr=mac, capacity=battery, executions=num_exec, consumption=consumption))

            # get device publishing info with query
            if i == utils._topic_device_indices[topic]:
                publishers._units[mac].addAssignment(topic, freq)
            publishers._units[mac].resetExecutions()
            publishers._units[mac].updateConsumption(publishers._units[mac].energyIncrease(task=None))

    for mac, device in publishers._units.items():
        # update db's executions
        db.updateDeviceExecutions(MAC_ADDR=mac, NEW_EXECUTIONS=device._numExecutions)
        db.updateDeviceConsumptions(MAC_ADDR=mac, NEW_CONSUMPTIONS=device._consumption)
        # update consumption

        # if there are no assignments given, give it the default value
        if not device._assignments:
            device._assignments = {"None":"None"}

        assignmentString = json.dumps(device._assignments)
        print(f"assignment string = {assignmentString}")
        publishers.addAssignmentsToCommand(deviceMac=mac, taskList=assignmentString)
        db.updatePublishTableWithPublishingAssignments(MAC_ADDR=mac, TOPICS=device._assignments.keys())
    
    db.closeDB()
    publishers.resetUnits()
    print(f"generated final command = {publishers._generated_cmd}")
    # while the publishers' unit information is reset, the assignments are preserved in generated_cmd
    return publishers._generated_cmd





        
