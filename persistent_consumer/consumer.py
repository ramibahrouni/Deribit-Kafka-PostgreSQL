import json
import os
import sys
from confluent_kafka import KafkaException, Consumer, KafkaError
from confluent_kafka.avro import AvroProducer
from loguru import logger
from kafka_schema_deribit import Deribit
from message import Message

broker = os.getenv('KAFKA_BROKER')
topic = os.getenv('KAFKA_TOPIC')
registry = os.getenv('SCHEMA_REGISTRY')
target_topic = os.getenv('KAFKA_PERSIST_TOPIC')
group_id = "laevitas-1"
fields_to_delete = ["stats", "greeks"]

consumer_config = {
    'bootstrap.servers': broker,
    'group.id': group_id,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
}

consumer = Consumer(consumer_config)


def acked(err, msg):
    if err is not None:
        logger.error("Failed to deliver message: %s: %s" % msg.value().decode('latin1'), str(err))
    else:
        logger.info("Message produced: %s" % msg.value().decode('latin1'))


def load_avro_schema_from_class():
    value_schema: str = Deribit.avro_schema()
    return value_schema


def publisher(msg):
    producer_config = {
        "bootstrap.servers": broker,
        "schema.registry.url": registry
    }

    value_schema = load_avro_schema_from_class()

    try:
        producer = AvroProducer(producer_config, default_value_schema=value_schema)
        print(msg_parser(msg))
        producer.produce(topic=target_topic, headers=[("my-header1", "Value1")], value=json.loads(msg_parser(msg)))
        producer.flush()

    except KafkaException as e:
        logger.error('Kafka failure {}'.format(e))


def msg_parser(msg):
    value = json.loads(msg.value().decode('latin1'))
    # dict
    data = value.get('params', {}).get('data')
    for field in fields_to_delete:
        del data[field]
    # str
    data = json.dumps(data)
    # Message(object)
    data_decoded: dict = json.loads(data, object_hook=Message.from_json)

    # print(final_data)

    message = json.dumps(Message.object_mapper(data_decoded))
    return message


# def subscriber():
#
#     consumer.subscribe([topic])
#
#     try:
#         msg = consumer.poll(1)
#         while True:
#             # read single message at a time
#
#             if msg is None:
#                 sleep(5)
#                 continue
#             if msg.error():
#                 logger.error("Error reading message : {}".format(msg.error()))
#                 continue
#             logger.info("Sending Message to the topic ")
#             publisher(msg)
#             consumer.commit()
#     except Exception as ex:
#         logger.error("Kafka Exception : {}", ex)
#     finally:
#         logger.info("closing Persistence Consumer")
#         consumer.close()

def consume():
    logger.info("Start consuming message from {}".format(topic))
    try:
        consumer.subscribe([topic])
        while True:
            msg = consumer.poll(1)
            if msg is None: continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                publisher(msg)
                consumer.commit()
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


if __name__ == "__main__":
    # Configuration of loguru
    logger.add(sys.stderr, format="{thread.name} | {time} | {level} | {message}", serialize=True, enqueue=True)
    try:
        consume()
    except KeyboardInterrupt:
        pass
