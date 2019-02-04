from __future__ import absolute_import, unicode_literals
import json
from kombu import Connection, Queue
from kombu import Connection, Exchange, Queue, Producer, Consumer
from kombu.asynchronous import Hub
from kombu import uuid
from kombu.mixins import ConsumerProducerMixin

from functools import partial

from db import db_write
from db import db_read

hub = Hub()

exchange = Exchange('supreme-octo-eureka', 'direct', durable=False)
read_queue = Queue('db.read', exchange=exchange, routing_key='db.read')
write_queue = Queue('db.write', exchange=exchange, routing_key='db.write')


def on_write(message):
    print('Write received', message)
    body = json.loads(message.body)
    try:
        db_write(body['key'], body['value'])
    finally:
        message.ack()


def on_read(conn, message):
    body = json.loads(message.body)
    print('Read received', body)
    with Producer(conn) as producer:
        try:
            value = db_read(body['key'])
            print('Db read', body['key'], value)
            producer.publish(
                {'result': value},
                exchange='',
                routing_key=message.properties['reply_to'],
                correlation_id=message.properties['correlation_id'],
                serializer='json',
                retry=True)
        finally:
            message.ack()


def main():
    with Connection('amqp://guest:guest@localhost//') as conn:
        conn.register_with_event_loop(hub)
        with Consumer(conn, [write_queue], on_message=on_write) as consumer:
            with Consumer(conn, [read_queue], on_message=partial(on_read, conn)) as consumer:
                hub.run_forever()


if __name__ == '__main__':
    main()
