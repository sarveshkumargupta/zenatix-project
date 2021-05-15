# **Zenatix Project**

# *Edge and Server Program*

This repository contains zenatix project solution code.

It has two files:
* [server_program]()
* [edge_program]()

## Installing the libraries
Install all required libraries using pip with requirement.txt of this repository.
```buildoutcfg
pip install -r requirements.txt
```
After you install all the dependencies, please run the server program first and then run edge program using python3.

## About Server Program
This program is listens to topic **devices/+/send** and then send the acknowledgment to **device/+/ack**.
It also response with success or failure randomly, if response is success then data will be stored in CSV.

## About Edge Program
This program simulates **temperature, humidity and moisture** sensor data with random float value, it also gets a time
stamp from Python's datetime module.
After consolidating a data in JSON format it sends that to cloud, now depends upon server response it stores data into
buffered list (queue concept). Using multithreading main function will send live and buffered data both at same time
with 60 seconds of interval.
