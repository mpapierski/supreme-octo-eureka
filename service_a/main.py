import time
import sys
import os
import shlex
from kombu import Connection, Exchange, Queue, Consumer
from functools import partial
from kombu import uuid
from kombu.asynchronous import Hub
from kombu import Connection, Exchange, Queue, Producer, Consumer

hub = Hub()

exchange = Exchange('supreme-octo-eureka', 'direct', durable=False)
read_queue = Queue('db.read', exchange=exchange, routing_key='db.read')
write_queue = Queue('db.write', exchange=exchange, routing_key='db.write')


def db_write(conn, key, value):
    producer = conn.Producer(serializer='json')
    producer.publish({
        'key': key,
        'value': value
    }, exchange=exchange, routing_key='db.write', declare=[write_queue])
    print('Ok')


class RPCClient(object):

    def __init__(self, connection):
        self.connection = connection
        self.callback_queue = Queue(uuid(), exclusive=True, auto_delete=True)

    def on_response(self, message):
        if message.properties['correlation_id'] == self.correlation_id:
            self.response = message.payload

    def call(self, key):
        self.response = None
        self.correlation_id = uuid()
        with Producer(self.connection) as producer:
            producer.publish(
                {'key': key},
                exchange='',
                routing_key='db.read',
                declare=[self.callback_queue],
                reply_to=self.callback_queue.name,
                correlation_id=self.correlation_id)
        with Consumer(self.connection,
                      on_message=self.on_response,
                      queues=[self.callback_queue], no_ack=True):
            while self.response is None:
                self.connection.drain_events()
        return self.response['result']


def db_read(conn, key):
    rpc = RPCClient(conn)
    return rpc.call(key)


def on_command(conn, line):
    cmd, args = shlex.split(line)
    if cmd == 'write':
        key, value = args.split('=')
        print('Key {} Value {}'.format(key, value))
        db_write(conn, key, value)
    elif cmd == 'read':
        key = args
        result = db_read(conn, key)
        print('Result', result)


buffer = b''


def stdin_callback(conn):
    global buffer
    data = os.read(sys.stdin.fileno(), 1024)
    if not data:
        print('EOF')
        hub.stop()
    buffer += data
    index = buffer.find(b'\n')
    if index != -1:
        command = buffer[:index].decode('utf-8')
        buffer = buffer[index + 1:]
        on_command(conn, command)


def main():
    with Connection('amqp://guest:guest@localhost//') as conn:
        conn.register_with_event_loop(hub)
        hub.add_reader(sys.stdin.fileno(), stdin_callback, conn)
        hub.run_forever()


if __name__ == '__main__':
    main()
