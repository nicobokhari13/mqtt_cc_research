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
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                break

class AsyncMqtt:
    def __init__(self, loop):
        self.loop = loop
        self.tasks = set()

    def on_connect(self, client, userdata, flags, rc):
        if rc == 5:
            sys.exit()
        client.subscribe(utils._CMD_TOPIC, qos=1)
        print("This on_connect publisher function gets called sala!!")

    async def waitForCmd(self):
        cmd = await self.got_message
        return cmd

    def on_message(self, client, userdata, msg):
        if mqtt.topic_matches_sub(msg.topic, utils._CMD_TOPIC):
            print(f"{utils._deviceMac} received command: {msg.payload.decode()} this is the topic the devices is subscribed to: {msg.topic}")
            self.got_message.set_result(msg.payload.decode())

    def on_disconnect(self, client, userdata, rc):
        self.disconnected.set_result(rc)

    async def waitForStatus(self):
        while True:
            print("starting window")

            await asyncio.sleep(utils._timeWindow)

            print("end window, sending status now")

            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Get the Battery Information
            if utils._IN_SIM:
                if not utils.decreaseSimEnergy():
                    print("battery is 0, no longer executing")
                    sys.exit()
            else:
                utils.getExperimentEnergy()

            status_json = {
                "time": current_time,
                "deviceMac": utils._deviceMac,
                "battery": utils._battery,
                "cpu_temperature": "None",
                "cpu_utilization_percentage": "None",
                "memory_utilization_percentage": "None"
            }

            status_str = json.dumps(status_json)
            print("status =")
            print(status_str)
            # publish status to status topic
            self.client.publish(topic=utils._STATUS_TOPIC, payload=status_str, qos=1)
            print(f"{utils._deviceMac} publishing status")

    async def publish_to_topic(self, sense_topic, freq):
        msg = "1" * 500000
        while True:
            self.client.publish(topic=sense_topic, payload=msg, qos=1)
            await asyncio.sleep(freq)
            print(f"{utils._deviceMac} publishing on {sense_topic}")

    async def separateExecutionsAndAssignments(self, command: str):
        # find the comma
        index = command.rfind(',')
        assignments = command[:index].strip()
        consumption = command[index + 1:].strip()
        print(f"{utils._deviceMac} assignments {assignments}")
        print(f"consuming {consumption} every minute")
        print("=================")
        return assignments, consumption

    async def main(self):
        # Main execution
        self.disconnected = self.loop.create_future()

        # Initialize MQTT client and assign callbacks
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.got_message = None

        # Start the AsyncioHelper
        aioh = AsyncioHelper(self.loop, self.client)
        self.client.connect("localhost", 1885, keepalive=1000)
        self.client.socket().setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2048)

        self.got_message = self.loop.create_future()

        # Wait for the first publishing command
        if not utils._publishes:
            print("Waiting for a command to start publishing.")

            # Wait for the command to arrive
            cmd = await self.got_message
            print(f"Received command: {cmd}")

            # Parse the JSON command and extract details
            try:
                parsed_cmd = json.loads(cmd)  # Convert JSON string to a dictionary
                topic = parsed_cmd.get("topic")  # Extract the topic
                latency = parsed_cmd.get("latency")  # Extract the latency

                # Separate into a list of items
                cmd_list = [topic, latency]
                print(f"Parsed command as list: {cmd_list}")
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON command: {e}")
                return
            except KeyError as e:
                print(f"Missing expected key in command: {e}")
                return

            # Handle the parsed command and set publishing details
            utils.setPublishing({'topics': [{"topic": topic, "value": latency}]})

            # Create publishing routines
            if utils._publishes:
                routines = [
                    self.publish_to_topic(topic_detail['topic'], topic_detail['value'])
                    for topic_detail in utils._publishes.get('topics', [])
                ]
            else:
                routines = []

            # Initialize new publishing and status update tasks
            self.got_message = self.loop.create_future()
            for coro in routines:
                self.tasks.add(asyncio.create_task(coro))

            self.tasks.add(asyncio.create_task(self.waitForCmd()))
            self.tasks.add(asyncio.create_task(self.waitForStatus()))

        # Main loop for running tasks
        while True:
            try:
                print("Running tasks.")
                done, pending = await asyncio.wait(self.tasks, return_when=asyncio.FIRST_COMPLETED)

                # Process the completed task
                result = done.pop().result()
                print(f"Task completed with result: {result}")

                # Cancel remaining tasks
                for unfinished_task in pending:
                    unfinished_task.cancel()
                self.tasks = set()

                # Parse the new command
                try:
                    parsed_result = json.loads(result)
                    topic = parsed_result.get("topic")
                    latency = parsed_result.get("latency")

                    # Separate into a list
                    result_list = [topic, latency]
                    print(f"Parsed result as list: {result_list}")
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON result: {e}")
                    return
                except KeyError as e:
                    print(f"Missing expected key in result: {e}")
                    return

                # Update publishing details
                utils.setPublishing({'topics': [{"topic": topic, "value": latency}]})

                # Reinitialize publishing routines
                if utils._publishes:
                    routines = [
                        self.publish_to_topic(topic_detail['topic'], topic_detail['value'])
                        for topic_detail in utils._publishes.get('topics', [])
                    ]
                else:
                    routines = []

                for coro in routines:
                    self.tasks.add(asyncio.create_task(coro))

                # Reinitialize command and status tasks
                self.tasks.add(asyncio.create_task(self.waitForCmd()))
                self.tasks.add(asyncio.create_task(self.waitForStatus()))
                self.got_message = self.loop.create_future()

            except asyncio.CancelledError:
                print("Task cancelled.")

def run_async_publisher():
    print(f"{utils._deviceMac} Starting Publisher Context")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(AsyncMqtt(loop).main())
    loop.close()
    print(f"{utils._deviceMac} Finished")

if __name__ == "__main__":
    run_async_publisher()