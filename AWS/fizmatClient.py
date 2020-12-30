# python3
import paho.mqtt.client as mqtt
import json
from datetime import datetime
from pymongo import MongoClient

def add_institution(msg, tString):
    if len(tString) <= 255:
        jsonString = {"building": tString}
        msg.update(jsonString)

def add_room(msg, tString):
    if len(tString) <= 255:
        jsonString = {"room": tString}
        msg.update(jsonString)

def add_topic(msg, tString):
    if len(tString) <= 255:
        jsonString = {"topic": tString}
        msg.update(jsonString)

def replace_dot(jsonMSG):
    for key in list(jsonMSG):
        if "." in key:
            jsonMSG[key.replace(".",",")] = jsonMSG.pop(key)

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("TTT-WP1/#")

def on_message(client, userdata, msg):
    try:
        message = msg.payload.decode("utf-8")
        jsonMSG = json.loads(message)

        if len(msg.topic.split('/')) >= 2:
            if msg.topic.split('/')[1] == "model":
                db = mongoClient.envMonDB_test
                collection = db.sensorDataWP1
                collection.insert_one(jsonMSG)

            elif len(msg.topic.split('/')) >= 4:
                institution = msg.topic.split('/')[2]
                room = msg.topic.split('/')[3]

                replace_dot(jsonMSG)
                #add_institution(jsonMSG, institution)
                #add_room(jsonMSG, room)
                add_topic(jsonMSG, msg.topic)

                db = mongoClient.envMonDB_test
                collection = db.sensorDataWP1
                collection.insert_one(jsonMSG)
        else:
            print("No Room or Institution defined! Topic: " + msg.topic)
    except:
        message = str(msg.payload)
        now = datetime.now()
        print("Exception!\nTime: " + str(now) + "\nTopic: " + msg.topic + "\nMessage: " + message + "\nEnd of exception")

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

