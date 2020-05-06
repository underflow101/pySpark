# Regular Expression with Spark

import re
from pyspark import SparkContext, SparkConf, RDD

conf = SparkConf().setAppName('Word Counts')
sc = SparkContext(conf = conf)
doc = sc.textFile("file:///opt/spark/data/shakespeare.txt")

flattened = doc.filter(lambda line: len(line) > 0).flatMap(lambda line: re.split('\W+', line))
flattened.take(6)

kvpairs = flattened.filter(lambda word: len(word) > 0).map(lambda word:(word.lower(), 1))
kvpairs.take(5)
countsbyword = kvpairs.reduceByKey(lambda v1, v2: v1 + v2).sortByKey(ascending=False)
countsbyword.take(5)

topwords = countsbyword.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)
topwords.take(5)
