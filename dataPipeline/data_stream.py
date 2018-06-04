import argparse
import atexit
import json
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import logging
logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-producer')
logger.setLevel(logging.DEBUG)


def process_stream(stream, kafka_producer, target_topic):
    def pair(data):
        """
        return (symbol, (timestamp, price, count))
        """
        record = json.loads(data.encode('utf-8'))
        return record.get('symbol'), (int(record.get('timestamp')), float(record.get('lastTradePrice')))

    def send_to_kafka(rdd):
        results = rdd.collect()
        for r in results:
            data = json.dumps({
                    'symbol': r[0],
                    'timestamp': int(round(time.time() * 1000)),
                    'open': r[1][1],
                    'close': r[2][1],
                    'high': r[3][1],
                    'low': r[4][1],
                    'average': r[5]
                })
            try:
                logger.info('Sending average price %s to kafka', data)
                kafka_producer.send(target_topic, value=data)
            except KafkaError as error:
                logger.warn('Failed to send average price to kafka, caused by: %s', error.message)

    stream.map(pair) \
        .groupByKey() \
        .map(lambda (k, v): (k, sorted(v, key=lambda x: x[0]))) \
        .map(lambda (k, v): (k, v[0], v[-1], max(v), min(v), sum([x[1] for x in v]) / len(v))) \
        .foreachRDD(send_to_kafka)


def shutdown_hook(producer):
    try:
        producer.flush(10)
    except KafkaError as kafka_error:
        logger.warn('Failed to flush pending messages, caused by: %s' % kafka_error)
    finally:
        try:
            producer.close(10)
        except Exception as e:
            logger.warn('Failed to close kafka connection, caused by %s' % e.message)


if __name__ == '__main__':
    # setup command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('source_topic', help='the kafka topic to subscribe from')
    parser.add_argument('target_topic', help='the kafka topic to send message to')
    parser.add_argument('kafka_broker', help='the location of the kafka broker')
    parser.add_argument('batch_duration', help='the location of the kafka broker')

    # parse arguments
    args = parser.parse_args()
    source_topic = args.source_topic
    target_topic = args.target_topic
    kafka_broker = args.kafka_broker
    batch_duration = int(args.batch_duration)

    # create SparkContext and SparkStreamingContext
    sc = SparkContext('local[2]', 'AveragePrice')
    sc.setLogLevel('INFO')
    ssc = StreamingContext(sc, batch_duration)

    # init a kafka stream for processing
    directKafkaStream = KafkaUtils.createDirectStream(ssc, [source_topic], {'metadata.broker.list': kafka_broker})

    # extract value
    stream = directKafkaStream.map(lambda x: x[1])

    # init a simple Kafka producer
    kafka_producer = KafkaProducer(bootstrap_servers=kafka_broker)

    process_stream(stream, kafka_producer, target_topic)

    # setup shutdown hook
    atexit.register(shutdown_hook, kafka_producer)

    ssc.start()
    ssc.awaitTermination()
