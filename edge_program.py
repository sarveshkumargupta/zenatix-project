import paho.mqtt.client as mqtt
import json
import time
from datetime import datetime
import random
import threading

# Do not change host, port and topic.
host_name = 'mqtt.eclipseprojects.io'
port = 1883
topic = 'devices/00121/send'  # Topic design is /devices/+/send

# Declaring global variables for publish result, count and buffered data.
pub_result = '{"response": null}'
sent_count = 0
buffered_data = []


def buffered_and_count():
    return f"Successfully transmitted data - {sent_count}, and Buffered data - {buffered_data}"


def simulate_sensor_data():
    """
    This function will generate random data for sensors and consolidate in JSON.

    :return: JSON (Simulated JSON data.)
    """
    # Defining candy factory sensors random value
    temperature = round(random.uniform(12.0, 49.0), 2)
    humidity = round(random.uniform(31.0, 83.0), 2)
    moisture = round(random.uniform(27.0, 67.0), 2)
    timestamp = datetime.utcnow()

    # JSON data
    telemetry_data = {
        "Sensor": ["temperature", "humidity", "moisture"],
        "Value": [temperature, humidity, moisture],
        "Timestamp": timestamp
    }
    return json.dumps(telemetry_data, default=str)


# The callback for when the client disconnect from the server.
def on_disconnect(client, userdata, rc):
    if rc == 0:
        print("Client disconnected.")
        client.connected_flag = False


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    client.subscribe("devices/00121/ack")
    if rc == 0:
        client.connected_flag = True
        print("Successfully connected.")
        print("Successfully message published.")
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


# The callback for when the client receives a Message id response from the server.
def on_publish(client, userdata, mid, qos=1):
    print("Mid value is {} and qos value is {}".format(mid, qos))


def on_message(client, userdata, msg):
    global pub_result
    pub_result = msg.payload.decode('utf-8')


# The callback for when the client receives a log response from the server.
def on_log(client, userdata, level, buf):
    print("MQTT Server/Broker: " + buf)


def send_buffered_data(client):
    """
    This function sends all the buffered data to cloud.

    :param client:
    :return: None
    """
    global sent_count, buffered_data
    if buffered_data:
        for data in buffered_data:
            result, mid = client.publish(topic, data, qos=1, retain=False)

            # Handling failure and success of data
            server_res = json.loads(pub_result).get("response")
            if result == 0 and server_res == 'successful':
                sent_count += 1
                buffered_data.remove(data)
            elif server_res == 'failed':
                buffered_data.append(data)


def send_live_data(client):
    """
    This functions collects live data and send to cloud.

    :param client:
    :return: result and mid value
    """
    global sent_count, buffered_data
    telemetry_data = simulate_sensor_data()
    result, mid = client.publish(topic, telemetry_data, qos=1, retain=False)

    # Handling failure and success of data
    server_res = json.loads(pub_result).get("response")
    if result == 0 and server_res == 'successful':
        sent_count += 1
    elif server_res == 'failed':
        buffered_data.append(telemetry_data)

    return telemetry_data, result, mid


def server_client_telemetry_run():
    """
    Making connection to MQTT Server/Broker with provided host_name.

    :return: None
    """
    global sent_count, buffered_data
    try:
        client.connect(host_name, port=port, keepalive=60)
    except Exception as ex:
        print("Connection failed: ", ex)
    except KeyboardInterrupt:
        print("Sample stopped")

    client.loop_start()
    while True:
        thread1 = threading.Thread(target=send_live_data, args=(client, ))
        thread2 = threading.Thread(target=send_buffered_data, args=(client, ))

        thread1.start()
        thread2.start()

        thread1.join()
        thread2.join()

        print("Done", sent_count, buffered_data)

        time.sleep(60)  # This value will publish message after every 60 sec once


if __name__ == '__main__':
    print("Started to connecting...")
    print("Press Ctrl+C to exit")

    client = mqtt.Client(clean_session=True)
    client.on_connect = on_connect
    client.on_log = on_log
    client.on_publish = on_publish
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    server_client_telemetry_run()
