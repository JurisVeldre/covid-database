# python3
import paho.mqtt.client as mqtt
import json
from datetime import datetime
from pymongo import MongoClient

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("VPP-APP/#")

def on_message(client, userdata, msg):
    message = msg.payload.decode("utf-8")

    try:
        jsonMSG = json.loads(message)

        db = mongoClient.envMonDB_test
        collection = db.appData
        collection.insert_one(jsonMSG)

    except:
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