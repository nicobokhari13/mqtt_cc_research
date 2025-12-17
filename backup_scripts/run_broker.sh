#!/bin/bash

cd /mnt/c/Users/sala_/OneDrive/Documents/MQTTRESEARCH/mqtt-cc-research/brokers/broker-mqtt-cc/mosquitto/src

#make binary so for now I am not going to compile the broker :)

# Check if make command was successful
if [ $? -eq 0 ]; then
    # Execute mosquitto with the provided configuration file
    pwd
    ./mosquitto -v -c mqtt_cc.conf
else
    echo "Make command failed. Exiting..."
    exit 1

cd ..

fi