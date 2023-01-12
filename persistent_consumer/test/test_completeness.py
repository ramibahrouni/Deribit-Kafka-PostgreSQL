"""
Since This test is all about Apache Kafka and PostgreSQL, we can use testcontainers as component provider for the components from docker-compose file
"""
import concurrent
import sys
from threading import Thread
from time import sleep
import asyncio
from confluent_kafka import Consumer, Producer, TopicPartition, KafkaError, KafkaException
from python_on_whales import docker
from testcontainers.postgres import PostgresContainer
from confluent_kafka.admin import AdminClient, NewTopic

# Tests are going to put in place to try kafka consuming/producing topic2 alongside with postgreSQL
# consumer(topic1) --> producer(topic2) --> postgres

topic_1 = 'test-1'
group_id_1 = 'grp-1'
broker = 'localhost:9092'

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
        'group.id': "test-1",
        'default.topic.config': {'auto.offset.reset': 'smallest', }}
consumer = Consumer(conf)

topic_list = []
topic_to_remove = []
msg_1 = 'This a message'
partition = 0


def create_topic():
    topic_list.append(NewTopic(topic_1, 1, 1))
    admin_client.create_topics(topic_list)


def del_topic():
    topic_to_remove.append(topic_1)
    admin_client.delete_topics(topic_to_remove)


def get_partition_size(topic_name: str, partition: int):
    topic_partition = TopicPartition(topic_name, partition)
    low_offset, high_offset = consumer.get_watermark_offsets(topic_partition)
    partition_size = high_offset - low_offset
    return partition_size


def write_msg(topic, msg):
    producer.produce(topic, msg, str(hash(msg)))
    producer.flush()


# First test will be conly about data integrity which means we are going to count data on each side
def write_to_topic(n: int):
    del_topic()
    sleep(0.5)
    create_topic()
    for x in range(n):
        write_msg(topic_1, msg_1 + str(x))
    counter = get_partition_size(topic_1, partition)
    print("Number of rows written into topic {}, is {}".format(topic_1, counter))
    return counter


def consume():
    counter = 0
    consumer.subscribe([topic_1])

    while True:

        msg = consumer.poll(1.0)
        counter = counter + 1
        consumer.commit(asynchronous=False)
        print(counter)



def test_1():
    read_thread = Thread(target=write_to_topic(1000))
    read_thread.start()
    write_thread = Thread(target=consume())
    write_thread.start()


if __name__ == '__main__':
    test_1()
