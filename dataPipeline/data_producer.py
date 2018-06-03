
import argparse
import requests
import schedule
import atexit
import time
import json

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError

import logging
logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-producer')
logger.setLevel(logging.DEBUG)

API_BASE = 'https://api.gdax.com'


def check_symbol(symbol):
    """
    helper method check if the symbol exists in coinbase API
    """
    logger.debug('Checking symbol ...')
    try:
        response = requests.get(API_BASE + '/products')
        product_ids = [product.get('id') for product in response.json()]

        if symbol not in product_ids:
            logger.warn('symbol %s is not supported. The list of supported symbol: %s' % (symbol, product_ids))
            exit()
    except Exception as e:
        logger.warn('Failed to fetch products.')


def fetch_price(symbol, producer, topic_name):
    """
    Helper function to retrieve data and send it to kafka
    """
    logger.debug('Start to fetch price for %s' % symbol)
    try:
        response = requests.get('%s/products/%s/ticker' % (API_BASE, symbol))
        price = response.json().get('price')
        timestamp = int(round(time.time() * 1000))
        payload = { 'symbol': str(symbol),
                    'lastTradePrice': str(price),
                    'timestamp': str(timestamp) }
        logger.debug('Retrieved %s info %s', symbol, payload)
        producer.send(topic=topic_name, value=json.dumps(payload), timestamp_ms=int(time.time())*1000)
        logger.debug('Sent price for %s to kafka' % symbol)
    except KafkaTimeoutError as timeout_error:
        logger.warn('Failed to send messages to kafka, caused by: %s' % timeout_error)
    except Exception as e:
        logger.warn('Failed to fech price: %s' % e.message)

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
    parser = argparse.ArgumentParser()
    parser.add_argument('symbol', help='the symbol you want to pull')
    parser.add_argument('topic_name', help='the kafka topic to push to')
    parser.add_argument('kafka_broker', help='the location of the kafka broker')

    # parse arguments
    args = parser.parse_args()
    symbol = args.symbol
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker

    # check if the symbol is supported
    check_symbol(symbol)

    # instantiate a simple kafka producer
    producer = KafkaProducer(bootstrap_servers=kafka_broker)

    # schedule and run fech_price function every second
    schedule.every(1).second.do(fetch_price, symbol, producer, topic_name)

    # setup shutdown hook
    atexit.register(shutdown_hook, producer)

    while 1:
        schedule.run_pending()
        time.sleep(1)
