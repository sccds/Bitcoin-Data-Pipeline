import json
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

import time
import data_stream
data_stream_module = data_stream

topic = 'test_topic'

# 42000 / 3 = 14000
test_input = [
    json.dumps({'timestamp': '1526900000001', 'symbol': 'BTC-USD', 'lastTradePrice': '10000'}),
    json.dumps({'timestamp': '1526900000002', 'symbol': 'BTC-USD', 'lastTradePrice': '12000'}),
    json.dumps({'timestamp': '1526900000003', 'symbol': 'BTC-USD', 'lastTradePrice': '20000'})
]

class TestKafkaProducer():
    def __init__(self):
        self.target_topic = None
        self.value = None

    def send(self, target_topic, value):
        self.target_topic = target_topic
        self.value = value

    def log(self):
        print 'target_topic: %s, value: %s' % (self.target_topic, self.value)


def _is_close(a, b, rel_tol=1e-09, abs_tol=0.0):
    return abs(a - b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)


def _make_dstream_helper(sc, ssc, input):
    input_rdds = [sc.parallelize(test_input, 1)]
    return ssc.queueStream(input_rdds)


def test_data_stream(sc, ssc, topic):
    input_stream = _make_dstream_helper(sc, ssc, test_input)
    kafka_producer = TestKafkaProducer()
    data_stream_module.process_stream(input_stream, kafka_producer, topic)

    ssc.start()
    time.sleep(5)
    ssc.stop()

    json_res = json.loads(kafka_producer.value)
    print json_res
    assert _is_close(json_res.get('startPrice'), 10000.0)
    assert _is_close(json_res.get('endPrice'), 20000.0)
    assert _is_close(json_res.get('maxPrice'), 20000.0)
    assert _is_close(json_res.get('minPrice'), 10000.0)
    assert _is_close(json_res.get('averagePrice'), 14000.0)
    print 'test_data_stream passed'


if __name__ == '__main__':
    conf = SparkConf().setMaster('local[2]').setAppName('local-testing')
    spark_context = SparkContext(conf=conf)
    streaming_context = StreamingContext(spark_context, 1)

    test_data_stream(spark_context, streaming_context, topic)
