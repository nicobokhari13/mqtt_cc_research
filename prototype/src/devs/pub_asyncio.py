import asyncio
import socket
import sys 
import paho.mqtt.client as mqtt
import pub_utils
import psutil
import json
from datetime import datetime

utils = pub_utils.PublisherUtils()

class AsyncioHelper:
    def __init__(self, loop, client):
        self.loop = loop
        self.client = client
        self.client.on_socket_open = self.on_socket_open
        self.client.on_socket_close = self.on_socket_close
        self.client.on_socket_register_write = self.on_socket_register_write
        self.client.on_socket_unregister_write = self.on_socket_unregister_write
        self.start_up = True

    def on_socket_open(self, client, userdata, sock):

        def cb():
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
                await asyncio.sleep(5)   
            except asyncio.CancelledError:
                break

class AsyncMqtt:
    def __init__(self, loop):
        self.loop = loop
        self.tasks = set()

    def on_connect(self, client, userdata, flags, rc):
        if(rc == 5):
            sys.exit()
        client.subscribe(utils._CMD_TOPIC)

    async def waitForCmd(self):
        cmd = await self.got_message
        return cmd

    def on_message(self, client, userdata, msg):
        if mqtt.topic_matches_sub(msg.topic, utils._CMD_TOPIC):
            print(f"{utils._deviceMac} received command: {msg.payload.decode()}")
            self.got_message.set_result(msg.payload.decode())

    def on_disconnect(self, client, userdata, rc):
        self.disconnected.set_result(rc)

    async def waitForStatus(self):
        while True:
            print("starting window")

            await asyncio.sleep(utils._timeWindow)

            print("end window, sending status now")

            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            print(f"time {current_time}")

            # Get the Battery Information
            if utils._IN_SIM:
                if not utils.decreaseSimEnergy():
                    print("battery is 0, no longer executing")
                    sys.exit()
            else:
                utils.getExperimentEnergy()
                
            print(f"battery: {utils._battery}")

            status_json = {
                "time": current_time,
                "deviceMac": utils._deviceMac,
                "battery": utils._battery,
                #"cpu_temperature": utils.get_cpu_temperature(),
                "cpu_temperature": "None",
                "cpu_utilization_percentage": utils.get_cpu_utilization(),
                "memory_utilization_percentage": utils.get_memory_utilization()
            }

            status_str = json.dumps(status_json)

            # publish status to status topic

            print("publishing status")

            self.client.publish(topic = utils._STATUS_TOPIC, payload = status_str)

    async def publish_to_topic(self, sense_topic, freq):
        msg = "1"
        while True:
            self.client.publish(topic = sense_topic, payload = msg)
            await asyncio.sleep(freq)
            print(f"finished publishing on {sense_topic} on freq {freq}")
    
    async def separateExecutionsAndAssignments(self, command:str):
        # find the comma
        print(command)
        index = len(command) - 1
        while index >= 0:
            if command[index] == ",":
                break
            index -= 1
        assignments = command[:index]
        executions = command[index + 1:]
        print(f"assignments {assignments}")
        print(f"executing {executions} every minute")
        utils.saveNewExecutions(executions)
        return assignments

    async def main(self):
        # main execution        
        self.disconnected = self.loop.create_future()

        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message 
        self.client.on_disconnect = self.on_disconnect
        self.got_message = None

        # set other necessary parameters for the client
        #self.client.username_pw_set(username=utils._USERNAME, password=utils._PASSWORD)
        aioh = AsyncioHelper(self.loop, self.client)
        self.client.connect("localhost", 1883)
        self.client.socket().setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2048)
        
        self.got_message = self.loop.create_future()
        
        if not utils._publishes: 

            # if nothing to publish yet (at start up)
            print("waiting for publish")
            # cmd = await utils._got_cmd # wait for command to come
            cmd = await self.got_message # wait for command to come
            # if in the sim, get separate the executions 
            if utils._IN_SIM:
                cmd = await self.separateExecutionsAndAssignments(cmd)
            # once we have command, set publishings
            utils.setPublishing(json.loads(cmd))

            # create sensing_task routines
            routines = [self.publish_to_topic(topic, freq) for topic,freq in utils._publishes.items()]

            # reset command
            self.got_message = self.loop.create_future()
            
            # tasks are the publishing tasks assigned to the publisher
            for coro in routines: 
                self.tasks.add(asyncio.create_task(coro))
            
            # also add waiting for command from prototype
            self.tasks.add(asyncio.create_task(self.waitForCmd()))
            self.tasks.add(asyncio.create_task(self.waitForStatus()))

        while True: #infinite loop
            try:
                print("running tasks")
                done, pending = await asyncio.wait(self.tasks, return_when=asyncio.FIRST_COMPLETED)
                # run the tasks until 1 completes

                # get the "returned" value from the done task
                result = done.pop().result()
                # sensing tasks return None, waitForCmd returns the command
                print(f"message says {result}")
                print(f"task result = {result}")
                print("result is command")
                print("canceling other tasks")
                # cancel other sensing tasks

                for unfinished_task in pending:
                    unfinished_task.cancel()
                    self.tasks = set()

                # check if simulation, if so, get the num executions out of the command
                if utils._IN_SIM:
                    result = await self.separateExecutionsAndAssignments(result)
                print(f"command is {result}")
                    # save executions in utils
                print("changing tasks")
                utils.setPublishing(json.loads(result))
                routines = [self.publish_to_topic(topic, freq) for topic,freq in utils._publishes.items()]
                #self.tasks = [asyncio.create_task(coro) for coro in routines]
                for coro in routines: 
                    self.tasks.add(asyncio.create_task(coro))
                self.tasks.add(asyncio.create_task(self.waitForCmd()))
                self.tasks.add(asyncio.create_task(self.waitForStatus()))
                # reset got cmd
                #utils._got_cmd = self.loop.create_future()
                self.got_message = self.loop.create_future()
                    # tasks are the publishing tasks assigned to the publisher
                    # also add waiting for command from prototype
            except asyncio.CancelledError:
                print("asyncio cancelled")

def run_async_publisher():
    print("Starting")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(AsyncMqtt(loop).main())
    loop.close()
    print("Finished")

if __name__ == "__main__":
    run_async_publisher()

