"""
Since This test is all about Apache Kafka and PostgreSQL, we can use testcontainers as component provider for the components from docker-compose file
"""
# Tests are going to put in place to try kafka consuming/producing topic2 alongside with postgreSQL
# producer(topic_1) --> consumer(topic_1) --> producer(topic_2) --> postgres


import json
import sys
from multiprocessing import Process
from time import sleep

import requests
import sqlalchemy as db
from confluent_kafka import Consumer, Producer, TopicPartition, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

from persistent_consumer.test.model.message_v1 import Message

topic_1 = 'deribit-1-test'
topic_2 = 'deribit_prices_test'
group_id_1 = 'grp-1'
broker = 'localhost:9092'
registry = 'http://localhost:8081'
connect = 'http://localhost:8083/connectors'
fields_to_delete = ["stats", "greeks"]

admin_client = AdminClient({
    "bootstrap.servers": broker
})

producer = Producer({
    'bootstrap.servers': broker}
)


def commit_completed(err, partitions):
    if err:
        print(str(err))
    else:
        print("Committed partition offsets: " + str(partitions))


conf = {'bootstrap.servers': broker,
        'group.id': group_id_1,
        'default.topic.config': {'auto.offset.reset': 'smallest', }}
consumer = Consumer(conf)

topic_list = []
topic_to_remove = []
partition = 0


def count_query():
    sleep(5)
    engine = db.create_engine("postgres://laevitas:laevitas@localhost:5433/laevitas")

    # CREATE THE METADATA OBJECT TO ACCESS THE TABLE
    meta_data = db.MetaData(bind=engine)
    db.MetaData.reflect(meta_data)

    # GET THE `actor` TABLE FROM THE METADATA OBJECT
    test_table = meta_data.tables[topic_2]

    # SELECT COUNT(*) FROM Actor
    result = db.select([db.func.count()]).select_from(test_table).scalar()
    print("Count:", result)
    return result


def get_data():
    f = open('data/deribit-channel.json')
    msg_1 = json.load(f)
    f.close()
    return msg_1


def create_topic(topic):
    topic_list.append(NewTopic(topic, 1, 1))
    admin_client.create_topics(topic_list)


def del_topic(topic):
    topic_to_remove.append(topic)
    admin_client.delete_topics(topic_to_remove)


def get_partition_size(topic_name: str, partition: int):
    topic_partition = TopicPartition(topic_name, partition)
    low_offset, high_offset = consumer.get_watermark_offsets(topic_partition)
    partition_size = high_offset - low_offset
    return partition_size


def write_msg(topic, msg):
    producer.produce(topic, json.dumps(msg), str(hash(json.dumps(msg))))
    producer.flush()


# Write method for the first topic where we will insert raw data coming from Deribit api
def write_to_topic_1(n: int):
    msg = get_data()
    for x in range(n):
        write_msg(topic_1, msg)
    counter = get_partition_size(topic_1, partition)
    print("Number of rows written into topic {}, is {}".format(topic_1, counter))
    return counter


def topic_ops(topic):
    del_topic(topic)
    sleep(0.5)
    create_topic(topic)


def connector_ops():
    url = connect
    rs = requests.get(url + "/" + topic_2).json()
    if 'error_code' in rs:
        payload = {
            "name": topic_2,
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
                "connection.attempts": 600,
                "connection.backoff.ms": 600000,
                "tasks.max": "1",
                "topics": topic_2,
                "connection.url": "jdbc:postgresql://postgres:5432/laevitas",
                "connection.user": "laevitas",
                "connection.password": "laevitas",
                "dialect.name": "PostgreSqlDatabaseDialect",
                "batch.size": 20,
                "auto.evolve": "true",
                "auto.create": "true",
                "max.retries": 150,
                "retry.backoff.ms": 30000
            }
        }
        print(payload)
        headers = {
            "Content-Type": "application/json"
        }
        res = requests.post(url, data=json.dumps(payload), headers=headers)
        print(res.json())


def load_avro_schema_from_file():
    value_schema: str = open('schema_files/topic_schema-v1.avsc').read()
    return value_schema


# Write method for the second topic where we will insert enriched data to be persisted into postgres
def write_to_topic_2(msg):
    producer_config = {
        "bootstrap.servers": broker,
        "schema.registry.url": registry
    }

    value_schema = load_avro_schema_from_file()

    try:
        producer = AvroProducer(producer_config, default_value_schema=value_schema)

        producer.produce(topic=topic_2, headers=[("my-header1", "Value1")], value=json.loads(msg_parser(msg)))
        producer.flush()
        counter = get_partition_size(topic_2, partition)
        print("Number of rows written into topic {}, is {}".format(topic_2, counter))
    except KafkaException as e:
        print('Kafka failure {}'.format(e))


def msg_parser(msg):
    value = json.loads(msg.value().decode('latin1'))
    # dict
    data = value.get('params', {}).get('data')
    for field in fields_to_delete:
        if field in data:
            del data[field]
    # str
    data = json.dumps(data)
    # Message(object)
    data_decoded: dict = json.loads(data, object_hook=Message.from_json)

    # print(final_data)

    message = json.dumps(Message.object_mapper(data_decoded))
    return message


def consume():
    try:
        consumer.subscribe([topic_1])

        while True:
            msg = consumer.poll(0.2)
            if msg is None: continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                write_to_topic_2(msg)
                consumer.commit()
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def test():
    topic_ops(topic_1)
    topic_ops(topic_2)
    write_to_topic_1(10)
    connector_ops()
    consume()


if __name__ == '__main__':
    test()
