#!/bin/bash

mosquitto_sub -h localhost -t "00:33:44/tasks=Temperature,Humidity;Max_Latency=89,88;Accuracy=90.9,78.7;Min_Frequency=4,5;"
#mosquitto_sub -h localhost -t "00:33:44/%tasks%@Temperature@Humidity%Max_Latency%@89@88%Accuracy%@90.9@78.7%Min_Freqency%@4@5"
