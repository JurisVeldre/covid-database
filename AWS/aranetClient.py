# python3
import paho.mqtt.client as mqtt
import json
from datetime import datetime
from pymongo import MongoClient

def add_id(msg, sensorid):
    sensor = {"sensor_id": sensorid}
    msg.update(sensor)

def add_alarm_type(msg, alarmType):
    alarm = {"alarm_type": alarmType}
    msg.update(alarm)

def add_alarm_msg(msg, variable, value):
    alarmMSG = {variable: value}
    msg.update(alarmMSG)

def add_timestamp(msg, value):
    alarmMSG = {"timestamp": value}
    msg.update(alarmMSG)

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("VPP-Aranet/#")

def on_message(client, userdata, msg):
    message = msg.payload.decode("utf-8")

    try:
        jsonMSG = json.loads(message)

        sensorid = msg.topic.split('/')[3]
        add_id(jsonMSG, sensorid)

        db = mongoClient.envMonDB_test
        collection = db.sensorDataAranet
        collection.insert_one(jsonMSG)

    except:
        jsonAlarm = {}
        now = datetime.now().timestamp()
        if msg.topic.split('/')[2] == "name":
            print("Aranet Pro Base " + message + " connected")

        elif msg.topic.split('/')[4] == "name":
            print("Sensor " + message + " alert")

        elif msg.topic.split('/')[4] == "alarms":
            add_id(jsonAlarm, msg.topic.split('/')[3])
            add_alarm_type(jsonAlarm, msg.topic.split('/')[5])
            add_alarm_msg(jsonAlarm, msg.topic.split('/')[6], message)
            add_timestamp(jsonAlarm, now)

            db = mongoClient.envMonDB_test
            collection = db.aranetAlerts
            collection.insert_one(jsonAlarm)

        else:
            print("Exception! Time: " + str(now) + "\nTopic: " + msg.topic + "\nMessage: " + message)

# Set up client for MongoDB
mongoClient = MongoClient(db_host, username = db_user, password = db_passwrd, authMechanism = 'SCRAM-SHA-256')

# Initialize the client that should connect to the Mosquitto broker
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.tls_set('/etc/mosquitto/certs/cert.pem', tls_version=2)
client.username_pw_set(username = name, password = passwrd)
client.connect(host, 8883)

# Blocking loop to the Mosquitto broker
client.loop_forever()

