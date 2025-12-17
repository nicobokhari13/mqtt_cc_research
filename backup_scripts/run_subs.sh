#!/bin/bash
cd /home/devnico/repos/research/prototype/src/subs
python3 subscriber.py sub01 mqttcc01 sensor/temperature%latency%20,sensor/humidity%latency%37,sensor/airquality%latency%50 &
python3 subscriber.py sub02 mqttcc02 sensor/temperature%latency%45,sensor/humidity%latency%30 &