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


from flask import Flask, request, Response
app = Flask(__name__)

@app.route("/db", methods=['POST'])
def db_write():
    with Connection('amqp://guest:guest@localhost//') as conn:
        conn.register_with_event_loop(hub)
        producer = conn.Producer(serializer='json')
        producer.publish({
            'key': request.form['key'],
            'value': request.form['value'],
        }, exchange=exchange, routing_key='db.write', declare=[write_queue])
        return Response('', status=201)

@app.route('/db/<key>', methods=['GET'])
def db_read(key):
    with Connection('amqp://guest:guest@localhost//') as conn:
        rpc = RPCClient(conn)
        result = rpc.call(key)
        if result is None:
            return '', 404
        else:
            return result, 200