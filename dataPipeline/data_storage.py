
import argparse
import atexit
import happybase
import json
from kafka import KafkaConsumer


import logging
logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(form=logger_format)
logger = logging.getLogger('data_storage')
logger.setLevel(logging.DEBUG)

# default values
topic_name = 'analyzer'
data_table = 'cryptocurrency'
hbase_host = 'myhbase'


def persist_data(data, hbase_connection, data_table):
    """
    Persist data into hbase
    """
    try:
        logger.debug('Start to persist data to hbase %s', data)
        parsed = json.loads(data)
        symbol = parsed.get('symbol')
        price = float(parsed.get('lastTradePrice'))
        timestamp = parsed.get('timestamp')

        table = hbase_connection.table(data_table)
        row_key = '%s-%s' % (symbol, timestamp)
        logger.info('Storing values with row key %s' % row_key)
        table.put(row_key, {'family:symbol' : str(symbol),
                            'family:timestamp' : str(timestamp),
                            'family:price' : str(price)})
        logger.info('Persisted data to hbase for symbol: %s, price: %f, timestamp: %s', symbol, price, timestamp)
    except Exception as e:
        logger.error('Failed to persist data to hbase for %s', e)


def shutdown_hook(consumer, connection):
    """
    a shutdown hook to be called before the shutdown
    """
    try:
        logger.info('Closing Kakfa consumer')
        consumer.close()
        logger.info('Kafka consumer closed')
        logger.info('Closing Hbase connection')
        connection.close()
        logger.info('Hbase connection closed')
    except Exception as e:
        logger.warn('Failed to close consumer/connection, caused by: %s', e)
    finally:
        logger.info('Exiting program')


if __name__ == '__main__':
    # setup command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('topic_name', help='the kafka topic to push to')
    parser.add_argument('kafka_broker', help='the location of the kafka broker')
    parser.add_argument('data_table', help='the data table to use')
    parser.add_argument('hbase_host', help='the host name of hbase')

    # parse arguments
    args = parser.parse_args()
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker
    data_table = args.data_table
    hbase_host = args.hbase_host

    # init a simple kafka consumer
    consumer = KafkaConsumer(topic_name, bootstrap_servers=kafka_broker)
    logger.debug("consumer init done")

    # init habase connection
    hbase_connection = happybase.Connection(hbase_host)
    logger.debug("habse connect init done")

    # create table if not exists
    logger.debug(hbase_connection.tables())
    if data_table not in hbase_connection.tables():
        hbase_connection.create_table(data_table, { 'family': dict() } )

    # setup proper shutdown hook
    atexit.register(shutdown_hook, consumer, hbase_connection)

    for msg in consumer:
        logger.debug(msg.value)
        persist_data(msg.value, hbase_connection, data_table)
