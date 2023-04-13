from paho.mqtt.client import Client
import paho.mqtt.publish as publish
from multiprocessing import Process
from time import sleep

def work_on_message(message, broker):
    print('process body', message)
    topic, timeout, text = message[2:-1].split(',')
    print('process body', timeout, topic, text)
    sleep(int(timeout))
    publish.single(topic, payload=text, hostname=broker)
    print('end process body',message)

def on_message(client, userdata, msg):
    print('on_message', msg.topic, msg.payload)
    worker = Process(target=work_on_message, args=(str(msg.payload), userdata['broker']))
    worker.start()
    print('end on_message', msg.payload)

def on_log(client, userdata, level, string):
    print("LOG", userdata, level, string)

def on_connect(client, userdata, flags, rc):
    print("CONNECT:", userdata, flags, rc)

def main(broker):
    userdata = {
        'broker': broker
    }
    client = Client(userdata=userdata)
    client.enable_logger()
    client.on_message = on_message
    client.on_connect = on_connect
    client.connect(broker)

    topic = 'clients/timeout'
    client.subscribe(topic)

    client.loop_forever()


if __name__ == "__main__":
    import sys
    if len(sys.argv)<2:
        print(f"Usage: {sys.argv[0]} broker")
        sys.exit(1)
    broker = sys.argv[1]
    main(broker)
