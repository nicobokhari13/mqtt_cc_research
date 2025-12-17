import asyncio
import threading
import socket
import sys 
import paho.mqtt.client as mqtt
from proto_utils import ProtoUtils
import proto_db as db
import status_handler as status
import will_topic_handler as will
import algo_handler as algo
import time
 
utils = ProtoUtils()

class AsyncioHelper:
    def __init__(self, loop, client):
        self.loop = loop
        self.client = client
        self.client.on_socket_open = self.on_socket_open
        self.client.on_socket_close = self.on_socket_close
        self.client.on_socket_register_write = self.on_socket_register_write
        self.client.on_socket_unregister_write = self.on_socket_unregister_write

    def on_socket_open(self, client, userdata, sock):

        def cb():
            #time.sleep(5)
            client.loop_read()

        self.loop.add_reader(sock, cb) 
        self.misc = self.loop.create_task(self.misc_loop())

    def on_socket_close(self, client, userdata, sock):
        self.loop.remove_reader(sock)
        self.misc.cancel()

    def on_socket_register_write(self, client, userdata, sock):

        def cb():
            client.loop_write()

        self.loop.add_writer(sock, cb) 
        
    def on_socket_unregister_write(self, client, userdata, sock):
        self.loop.remove_writer(sock)


    async def misc_loop(self):
        while self.client.loop_misc() == mqtt.MQTT_ERR_SUCCESS:
            try:
                print("in misc loop")
                await asyncio.sleep(15)                 
            except asyncio.CancelledError:
                break



class AsyncMqtt:
    def __init__(self, loop):
        self.loop = loop


    async def sendCommandToDevice(self, topic, msg):
        self.client.publish(topic, msg, qos=1)
        print(f"sent to topic {topic}")

    async def sendCommands(self, mapAssignments:dict):
        print("sending commands")
        command_threads = [self.sendCommandToDevice(topic=utils._CMD_TOPIC+macAddress, msg=cmd) for macAddress, cmd in mapAssignments.items()] 
        await asyncio.gather(*command_threads)
        # resolve ranAlgo object
        print("finished sending commands")

    def on_connect(self, client, userdata, flags, rc):
        if(rc == 5):
            sys.exit()
        self.subscribeToTopics([utils._STATUS_TOPIC, utils._SUBS_WILL_TOPIC, utils._NEW_SUBS_TOPIC, utils._LAT_CHANGE_TOPIC])

    def subscribeToTopics(self, topics):
        for topic in topics:
            self.client.subscribe(topic,qos=1)

    def on_message(self, client, userdata, msg):
        topic = msg.topic
        payload = msg.payload.decode()
        print("in on_message")
        print("-------------")

        # use thread instead
        if mqtt.topic_matches_sub(utils._STATUS_TOPIC, topic):
            print("in status handler")
            status.handle_status_msg(payload)

        # if mqtt.topic_matches_sub(utils._SUBS_WILL_TOPIC, topic):
        #     print("in will handler")
        #     will.updateDB(payload)
        #     mapAssignments = algo.generateAssignments()
        #     self.got_message.set_result(mapAssignments)

        # if mqtt.topic_matches_sub(utils._NEW_SUBS_TOPIC, topic):
        #     print("in new subs handler")
        #     print(f"payload = {payload}")
        #     # if there is a new topic, generate the new assignments
        #     mapAssignments = algo.generateAssignments()
        #     # resolve gotCmdToSend with the assignments
        #     print("before setting assignments")
        #     self.got_message.set_result(mapAssignments)
        #     print("after setting assignments")

        # if mqtt.topic_matches_sub(utils._LAT_CHANGE_TOPIC, topic):
        #     # the message payload holds the topic with the changed max_allowed_latency
        #     # algo handler should still generateAssignemnts, must handle case where max allowed latency of topic changed
        #     print("in lat change handler")
        #     print(f"payload = {payload}")
        #     mapAssignments = algo.generateAssignments(changedTopic=payload)
        #     print("before setting assignments")
        #     self.got_message.set_result(mapAssignments)
        #     print("after setting assignments")

    def on_disconnect(self, client, userdata, rc):
        self.disconnected.set_result(rc)

    async def waitForTimeWindow(self):
        print("waiting for time window")
        await asyncio.sleep(utils._timeWindow)
        # if time window is done first, resolve the got cmd to None
        print("ending window")
        return None
    
    async def appendExecutions(self, command):
        deviceExecutions = algo.getPublisherExecutions()
        deviceConsumptions = algo.getPublisherConsumptions()
        print(deviceExecutions)
        for device in deviceConsumptions:
            print(f"mac {device[0]} and conssumption = {device[1]}")
            print(type(device[0]))
            print(type(device[1]))
            if device[0] in command.keys():
                print("appending consumption")
                command[device[0]] = str(command[device[0]]) + "," + str(device[1]) 
                # append the device executions to the string with comma
        print(command)
        return command 
    

    # Assumption, subscribers don't leave the simulation, only are added
    async def lookForChange(self):
        database = db.Database() #takes in the database context.
        while True:
            await asyncio.sleep(180)    
            print("opening database")
            database.openDB()
            mapAssignments = None
            print(self.got_message)
            changedLatencyTopics = database.findChangedLatencyTopics()
            newTopics = database.findAddedTopics()
            if len(changedLatencyTopics) > 0 or len(newTopics) > 0:
                print(changedLatencyTopics)
                print(newTopics)
                update_list = []
                if changedLatencyTopics:
                    update_list += changedLatencyTopics
                if newTopics: 
                    update_list += newTopics
                print(update_list)
                database.resetAddedAndChangedLatencyTopics(update_list)
                mapAssignments = algo.generateAssignments()
            else:
                algo.resetPublishingsAndDeviceExecutions()
                mapAssignments = algo.generateAssignments()
            if mapAssignments:
                print("got assignments")
                print(mapAssignments)
                print("closing database")
                return mapAssignments
            print("closing database")
            print("going to sleep")


    async def main(self):
        # main execution        
        #manages and tracks the outcome of the loop
        self.disconnected = self.loop.create_future()#creates a future object tied to the event loop
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.got_message = None
        self.continue_next_msg = threading.Event()
        # set other necessary parameters for the client aka middleware

        #self.client.username_pw_set(username=utils._USERNAME, password=utils._PASSWORD)

        aioh = AsyncioHelper(self.loop, self.client)

        self.client.connect("localhost", 1885, keepalive=1000)# here is where it connects to the broker sala

        self.client.socket().setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2048)

        while True: #infinite loop
            self.got_message = self.loop.create_future()
            wait_for_cmd_routine = asyncio.ensure_future(self.lookForChange())#coroutine
            wait_for_window_routine = asyncio.create_task(self.waitForTimeWindow())
            done, pending = await asyncio.wait([wait_for_cmd_routine, wait_for_window_routine], return_when=asyncio.FIRST_COMPLETED)
            if wait_for_cmd_routine in done:
                result = wait_for_cmd_routine.result()
            elif wait_for_window_routine in done:
                result = wait_for_window_routine.result()
            print(f"the result of tasks is {result}")
            print(type(result))
            if result: # if result exists, then result holds the command
                # cancel what is still pending
                for task in pending:
                    task.cancel()
                if utils._in_sim:
                # if this is the simulation, we must append the number of executions for each publisher
                    result = await self.appendExecutions(result)
                await self.sendCommands(result)
                # utils._gotCmdToSend = None
            else: # if result is None, then the time Window expired, must run algo again
                # reset publishings to 0 and executions to 0 
                for task in pending:
                    task.cancel()
                algo.resetPublishingsAndDeviceExecutions()
                # run algo again
                mapAssignments = algo.generateAssignments()
                if utils._in_sim: 
                    mapAssignments = await self.appendExecutions(mapAssignments)
                # send command
                await self.sendCommands(mapAssignments)
            self.got_message = None
            print("end of loop")

def run_async_client():
    print("Starting")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(AsyncMqtt(loop).main())#Event loop is executed on the main thread's event loop.
    loop.close()
    print("Finished")

if __name__ == "__main__":
    run_async_client()

