from paho.mqtt.client import Client
from multiprocessing import Process, Manager
from time import sleep
import paho.mqtt.publish as publish
import time

def on_message(client, data, msg):
    print(f"MESSAGE:data:{data}, msg.topic:{msg.topic}, payload:{msg.payload}")


def on_log(client, userdata, level, string):
    print("LOG", userdata, level, string)

def main(broker):
    data = {'status':0}
    client = Client(userdata=data)
    client.enable_logger()
    client.on_message = on_message
    client.on_log = on_log
    client.connect(broker)

    res_topics = ['clients/a', 'clients/b']
    for t in res_topics:
        client.subscribe(t)
    client.loop_start()
    tests = [
        (res_topics[0], 4, 'uno'),
        (res_topics[1], 1, 'dos'),
        (res_topics[0], 2, 'tres'),
        (res_topics[1], 5, 'tres')
    ]
    topic = 'clients/timeout'
    for test in tests:
        client.publish(topic, f'{test[0]},{test[1]},{test[2]}')
    time.sleep(10)

if __name__ == "__main__":
    import sys
    if len(sys.argv)<2:
        print(f"Usage: {sys.argv[0]} broker")
        sys.exit(1)
    broker = sys.argv[1]
    main(broker)
