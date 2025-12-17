import asyncio  # Asynchronous programming library
import threading  # For threading-based concurrency
import socket  # For low-level network interfaces
import sys  # For system-specific parameters and functions
import paho.mqtt.client as mqtt  # MQTT library for communication
from proto_utils import ProtoUtils  # Custom utilities for the application
import proto_db as db  # Database module
import status_handler as status  # Status message handler
import will_topic_handler as will  # Will message handler
import algo_handler as algo  # Algorithm logic handler
import time  # For time-related operations
 
utils = ProtoUtils()  # Initialize ProtoUtils, a utility class

class AsyncioHelper:
    """
    Helper class to integrate Paho MQTT client with asyncio.
    """
    def __init__(self, loop: asyncio.AbstractEventLoop, client: mqtt.Client):
        """
        Initializes the AsyncioHelper class.
        
        Args:
            loop (asyncio.AbstractEventLoop): The asyncio event loop.
            client (mqtt.Client): The Paho MQTT client instance.
        """
        self.loop = loop
        self.client = client
        self.client.on_socket_open = self.on_socket_open
        self.client.on_socket_close = self.on_socket_close
        self.client.on_socket_register_write = self.on_socket_register_write
        self.client.on_socket_unregister_write = self.on_socket_unregister_write

    def on_socket_open(self, client: mqtt.Client, userdata, sock: socket.socket):
        """
        Callback for when the MQTT socket opens.
        Adds a reader for the socket to the event loop.
        """
        def cb():
            client.loop_read()

        self.loop.add_reader(sock, cb)
        self.misc = self.loop.create_task(self.misc_loop())

    def on_socket_close(self, client: mqtt.Client, userdata, sock: socket.socket):
        """
        Callback for when the MQTT socket closes.
        Removes the reader from the event loop.
        """
        self.loop.remove_reader(sock)
        self.misc.cancel()

    def on_socket_register_write(self, client: mqtt.Client, userdata, sock: socket.socket):
        """
        Callback for when the MQTT socket needs to write.
        Adds a writer for the socket to the event loop.
        """
        def cb():
            client.loop_write()

        self.loop.add_writer(sock, cb)
        
    def on_socket_unregister_write(self, client: mqtt.Client, userdata, sock: socket.socket):
        """
        Callback for when the MQTT socket no longer needs to write.
        Removes the writer from the event loop.
        """
        self.loop.remove_writer(sock)

    async def misc_loop(self):
        """
        Periodically calls the MQTT loop_misc function to handle keep-alives.
        """
        while self.client.loop_misc() == mqtt.MQTT_ERR_SUCCESS:
            try:
                print("in misc loop")
                await asyncio.sleep(15)
            except asyncio.CancelledError:
                break


class AsyncMqtt:
    """
    Class to handle MQTT client interactions with asyncio.
    """
    def __init__(self, loop: asyncio.AbstractEventLoop):
        """
        Initializes the AsyncMqtt class.
        
        Args:
            loop (asyncio.AbstractEventLoop): The asyncio event loop.
        """
        self.loop = loop

    async def sendCommandToDevice(self, topic: str, msg: str):
        """
        Publishes a command to a specific MQTT topic.
        
        Args:
            topic (str): The MQTT topic to publish to.
            msg (str): The message to publish.
        """
        self.client.publish(topic, msg, qos=1)
        print(f"sent to topic {topic}")

    async def sendCommands(self, mapAssignments: dict):
        """
        Sends multiple commands to devices based on assignments.
        
        Args:
            mapAssignments (dict): A mapping of device MAC addresses to command strings.
        """
        print("sending commands")
        command_threads = [
            self.sendCommandToDevice(topic=utils._CMD_TOPIC + macAddress, msg=cmd)
            for macAddress, cmd in mapAssignments.items()
        ]
        await asyncio.gather(*command_threads)
        print("finished sending commands")

    def on_connect(self, client: mqtt.Client, userdata, flags, rc: int):
        """
        Callback for when the MQTT client connects to the broker.
        """
        if rc == 5:  # Connection refused
            sys.exit()
        self.subscribeToTopics([
            utils._STATUS_TOPIC,
            utils._SUBS_WILL_TOPIC,
            utils._NEW_SUBS_TOPIC,
            utils._LAT_CHANGE_TOPIC
        ])

    def subscribeToTopics(self, topics: list):
        """
        Subscribes the client to a list of topics.
        
        Args:
            topics (list): A list of MQTT topics to subscribe to.
        """
        for topic in topics:
            self.client.subscribe(topic, qos=1)

    def on_message(self, client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):
        """
        Callback for when a message is received on a subscribed topic.
        """
        topic = msg.topic
        payload = msg.payload.decode()
        print("in on_message")
        print("-------------")

        # Handle messages based on topic
        if mqtt.topic_matches_sub(utils._STATUS_TOPIC, topic):
            print("in status handler")
            status.handle_status_msg(payload)

    def on_disconnect(self, client: mqtt.Client, userdata, rc: int):
        """
        Callback for when the MQTT client disconnects from the broker.
        """
        self.disconnected.set_result(rc)

    async def waitForTimeWindow(self) -> None:
        """
        Waits for a predefined time window.
        """
        print("waiting for time window")
        await asyncio.sleep(utils._timeWindow)
        print("ending window")

    async def appendExecutions(self, command: dict) -> dict:
        """
        Appends execution and consumption details to a command dictionary.
        
        Args:
            command (dict): The command dictionary to update.
        
        Returns:
            dict: The updated command dictionary.
        """
        deviceExecutions = algo.getPublisherExecutions()
        deviceConsumptions = algo.getPublisherConsumptions()
        print(deviceExecutions)
        for device in deviceConsumptions:
            if device[0] in command.keys():
                command[device[0]] = f"{command[device[0]]},{device[1]}"
        print(command)
        return command

    async def lookForChange(self) -> dict:
        """
        Monitors the database for changes and generates new assignments.
        
        Returns:
            dict: A mapping of device assignments.
        """
        database = db.Database()
        while True:
            await asyncio.sleep(180)
            database.openDB()
            mapAssignments = None
            changedLatencyTopics = database.findChangedLatencyTopics()
            newTopics = database.findAddedTopics()
            if len(changedLatencyTopics) > 0 or len(newTopics) > 0:
                update_list = changedLatencyTopics + newTopics
                database.resetAddedAndChangedLatencyTopics(update_list)
                mapAssignments = algo.generateAssignments()
            else:
                algo.resetPublishingsAndDeviceExecutions()
                mapAssignments = algo.generateAssignments()
            if mapAssignments:
                return mapAssignments

    async def main(self):
        """
        Main execution loop for the MQTT client.
        """
        self.disconnected = self.loop.create_future()
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect

        aioh = AsyncioHelper(self.loop, self.client)
        self.client.connect("localhost", 1885, keepalive=1000)
        self.client.socket().setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2048)

        while True:
            self.got_message = self.loop.create_future()
            wait_for_cmd_routine = asyncio.ensure_future(self.lookForChange())
            wait_for_window_routine = asyncio.create_task(self.waitForTimeWindow())
            done, pending = await asyncio.wait([wait_for_cmd_routine, wait_for_window_routine], return_when=asyncio.FIRST_COMPLETED)
            if wait_for_cmd_routine in done:
                result = wait_for_cmd_routine.result()
            elif wait_for_window_routine in done:
                result = wait_for_window_routine.result()
            if result:
                for task in pending:
                    task.cancel()
                if utils._in_sim:
                    result = await self.appendExecutions(result)
                await self.sendCommands(result)
            else:
                for task in pending:
                    task.cancel()
                algo.resetPublishingsAndDeviceExecutions()
                mapAssignments = algo.generateAssignments()
                if utils._in_sim:
                    mapAssignments = await self.appendExecutions(mapAssignments)
                await self.sendCommands(mapAssignments)

def run_async_client():
    """
    Runs the MQTT client in an asyncio event loop.
    """
    loop = asyncio.get_event_loop()
    loop.run_until_complete(AsyncMqtt(loop).main())
    loop.close()

if __name__ == "__main__":
    run_async_client()
