from paho.mqtt.client import Client
from multiprocessing import Process, Manager
from time import sleep
import random

NUMBERS = 'numbers'
CLIENTS = 'clients'
TIMER_STOP = f'{CLIENTS}/timerstop'
HUMIDITY = 'humidity'


def es_primo(n):
    i = 2
    while i*i < n and n % i != 0:
        i += 1
    return i*i > n


def timer(time, data):
    client = Client()
    client.connect(data['broker'])
    msg = f'timer working. timeout: {time}'
    print(msg)
    client.publish(TIMER_STOP, msg)
    sleep(time)
    msg = f'timer working. timeout: {time}'
    client.publish(TIMER_STOP, msg)
    print('timer end working')
    client.disconnect()


def on_message(client, data, msg):
    print(f"MESSAGE:data:{data}, msg.topic:{msg.topic}, payload:{msg.payload}")
    try:
        if int(msg.payload) % 2 == 0:
            worker = Process(target=timer,
                             args=(random.random()*10, data))
            worker.start()
    except ValueError as e:
        print(e)
        pass


def on_log(client, userdata, level, string):
    print("LOG", userdata, level, string)


def main(broker):
    data = {'client':None,
            'broker': broker}
    client = Client(client_id="combine_numbers", userdata=data)
    data['client'] = client
    client.enable_logger()
    client.on_message = on_message
    client.on_log = on_log
    client.connect(broker)
    client.subscribe(NUMBERS)
    client.loop_forever()


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} broker")
        sys.exit(1)
    broker = sys.argv[1]
    main(broker)
