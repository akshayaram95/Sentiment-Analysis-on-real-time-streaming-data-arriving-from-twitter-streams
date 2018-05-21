from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import json
from pycorenlp import StanfordCoreNLP
from elasticsearch import Elasticsearch
from datetime import datetime
import requests
headers = {'content-type': 'application/json'}
elasticsearch_index_uri = 'http://localhost:9200/twitter/tweet'
mapping = {
    "mappings": {
        "tweet": {
            "properties": {
                "text": {
                    "type": "keyword"
                },
                "sentiment": {
                    "type": "keyword"
                    },
                "location": {
                    "type": "geo_point"
                },
                "timestamp": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss"
                },
            }
        }
     }
}
es = Elasticsearch()
es.indices.delete(index='twitter')
# ignore 400 cause by IndexAlreadyExistsException when creating an index
es.indices.create(index='twitter', body=mapping, ignore=400)

TCP_IP = 'localhost'
TCP_PORT = 9001
# Pyspark
# create spark configuration
conf = SparkConf()
conf.setAppName('TwitterApp')
conf.setMaster('local[2]')
# create spark context with the above configuration
sc = SparkContext(conf=conf)
nlp = StanfordCoreNLP('http://localhost:9000')

def index_to_elasticsearch(x):
	#timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if x[2] =='' or x[3] == '' :
        doc = {
            'text': x[0],
            'sentiment': x[1],
            'timestamp': x[4]
        }
    else:
        doc = {
            'text': x[0],
            'sentiment': x[1],
            'location':{
            'lat': x[2],
            'lon': x[3]
            },
            'timestamp': x[4]
        }
	#requests.post(uri, data=doc)
    requests.post(elasticsearch_index_uri, data=json.dumps(doc), headers=headers)

def process(time, rdd):
    
    rdd.foreach(index_to_elasticsearch)
    
# create the Streaming Context from spark context with interval size 2 seconds
ssc = StreamingContext(sc,4 )
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 900
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)
######### your processing here ###################
words = dataStream.map(lambda x: x.split('~'))
print('----------------------------------')
	words.pprint()
try:	
	sentiment=words.map(lambda x:
						(x[0],nlp.annotate(x[0], properties={
            	           'annotators': 'sentiment',
        	               'outputFormat': 'json',
    	                   'timeout': 1000,
	                   })['sentences'][0]['sentiment'],x[1],x[2],x[3]))
	#sentiment.pprint()
	sentiment.foreachRDD(process)
except:
	print('')

#################################################
ssc.start()
ssc.awaitTermination()