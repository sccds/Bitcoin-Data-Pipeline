import argparse
import json
import atexit

from kafka import KafkaConsumer


def consume(topic_name, kafka_broker, symbol):
    consumer = KafkaConsumer(topic_name, bootstrap_servers=kafka_broker)
    atexit.register(shutdown_hook, consumer)
    for message in consumer:
        if symbol:
            if json.loads(message.value).get('symbol') == symbol:
                print message
        else:
            print message


def shutdown_hook(consumer):
    try:
        consumer.close()
    except Exception as e:
        print 'Failed to close consumer, caused by:', e
    finally:
        print 'Exiting program'


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('topic_name', help='the kafka topic to pull from')
    parser.add_argument('kafka_broker', help='the location of the kafka broker')
    parser.add_argument('--symbol', help='the bitcoin symbol')

    # parser arguments
    args = parser.parse_args()
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker
    symbol = args.symbol

    consume(topic_name, kafka_broker, symbol)
