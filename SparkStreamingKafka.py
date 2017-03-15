from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import time
import json

#def ascii_encode_dict(data):
#    ascii_encode = lambda x: x.encode('ascii')
#    return dict(map(ascii_encode, pair) for pair in data.items())
def helper(data):
    return data.encode('ascii')

def enc(data):
    result = {k:helper(v) for k, v in data.items()}
    return result

#return dict(map(lambda line: line.encode('ascii'), pair.value) for pair in data.items())

conf = SparkConf().setMaster("local[*]").setAppName("StreamingDirectKafka")
sc = SparkContext(conf = conf)
ssc = StreamingContext(sc, 10)
skQuorum = "localhost:2181"
topic = ["meetup"]
kafkaParams = {"metadata.broker.list":"localhost:9092"}

#, kafkaParams = {"metadata.broker.list":"localhost:9092"}
kafkaStream = KafkaUtils.createDirectStream(ssc, topic, kafkaParams)
#stream = ssc.receiverStream( \
#    MeetupReceiver("https://stream.meetup.com/2/rsvps") \
#)
"""
data = kafkaStream.map(lambda line: json.loads(line)
"""
rsvp = kafkaStream.map(lambda line: line[1])
rsvp2 = rsvp.map(lambda line: json.loads(line.encode("ascii", "ignore")))

#kafkaStream.pprint()
"""
process = data.mapValues(lambda line: line.encode('ascii')).cache()
"""

#event = data["topic_name"]
#print(data.pprint())

print (rsvp.pprint())
"""
print (data.mapValues(enc).pprint())
"""

#print (event.pprint())
ssc.start()
time.sleep(100)
ssc.stop(stopSparkContext=True, stopGraceFully=True)
"""
ssc.awaitTermination().stop(stopSparkContext=True, stopGraceFully=True)
"""

#bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8:2.1.0
#bin/spark-submit --jars <spark-streaming-kafka-0-8-assembly.jar>

