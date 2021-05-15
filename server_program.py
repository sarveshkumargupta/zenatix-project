import paho.mqtt.client as mqtt
import random
import csv
import json
import pathlib


def append_data_in_csv(message):
    """
    This function will append the message data in csv file
    :param message: dict
    :return: None
    """
    timestp = message.get('Timestamp')
    value = message.get('Value')
    sensor = message.get('Sensor')
    fieldnames = ["Timestamp", "Value", "Sensor"]
    file = pathlib.Path('dataset.csv')
    if file.exists():
        with open('dataset.csv', mode='a', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            for index in range(len(value)):
                payload = {'Timestamp': timestp, 'Value': value[index], 'Sensor': sensor[index]}
                writer.writerow(payload)
    else:
        with open('dataset.csv', mode='w', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for index in range(len(value)):
                payload = {'Timestamp': timestp, 'Value': value[index], 'Sensor': sensor[index]}
                writer.writerow(payload)


# The callback for when the client disconnect from the server.
def on_disconnect(client, userdata, rc):
    if rc == 0:
        print("Client disconnected.")
        client.connected_flag = False


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    client.subscribe("devices/+/send")
    if rc == 0:
        client.connected_flag = True
        print("Successfully connected.")
    elif rc == 1:
        print("Unacceptable protocol version of server.")
    elif rc == 2:
        print("Invalid client identifier.")
    elif rc == 3:
        print("Server is unavailable to connect.")
    elif rc == 4:
        print("Bad username or password or client_id.")
    elif rc == 5:
        print("You are not authorised to connect.")
    elif rc == 6:
        print("Currently server is unused.")


# The callback for when the client receives a log response from the server.
def on_log(client, userdata, level, buf):
    print("MQTT Server/Broker: " + buf)


def on_message(client, userdata, msg):
    pub_topic = msg.topic.replace('send', 'ack')
    msg_status = ["successful", "failed"]
    rdnm_nmb = random.randint(0, 1)
    response = {'response': msg_status[rdnm_nmb]}
    try:
        client.publish(pub_topic, json.dumps(response))
    except Exception as exp:
        print("Couldn't publish: ", exp)
    if response.get('response') == 'successful':
        message = json.loads(msg.payload)
        append_data_in_csv(message)


if __name__ == '__main__':
    print("Started to connecting...")
    print("Press Ctrl+C to exit")
    mqttBroker = "mqtt.eclipseprojects.io"
    client = mqtt.Client()
    try:
        client.connect(mqttBroker)
    except Exception as ex:
        print("Connection failed: ", ex)
    client.on_connect = on_connect
    client.on_log = on_log
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.loop_forever(timeout=1.0, max_packets=1, retry_first_connection=True)
