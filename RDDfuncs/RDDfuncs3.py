# Spark RDD functions 3

import sys, re
from pyspark import SparkConf, SparkContext, RDD

conf = SparkConf().setAppName('RDD Func')
sc = SparkContext(conf=conf)

# RDD.union(<otherRDD>)
odds = sc.parallelize([1, 3, 5, 7, 9])
fibonacci = sc.parallelize([0, 1, 2, 3, 5, 8])
odds.union(fibonacci).collect()

# RDD.intersection(<otherRDD>)
odds = sc.parallelize([1, 3, 5, 7, 9])
fibonacci = sc.parallelize([0, 1, 2, 3, 5, 8])
odds.intersection(fibonacci).collect()

# RDD.subtract(<otherRDD>, numPartitions=None)
odds = sc.parallelize([1, 3, 5, 7, 9])
fibonacci = sc.parallelize([0, 1, 2, 3, 5, 8])
odds.subtract(fibonacci).collect()

# RDD.subtractByKey(<otherRDD>, numPartitions=None)
cities1 = sc.parallelize([('Hayward', (37.668819, -122.080795)),
                          ('Baumholder', (49.6489, 7.3975)),
                          ('Alexandria', (38.820450, -77.050552)),
                          ('Melbourne', (37.663712, 144.844788))])
cities2 = sc.parallelize([('Boulder  Creek', (64.0708333, -148.2236111)),
                          ('Hayward', (37.668819, -122.080795)),
                          ('Alexandria', (38.820450, -77.050552)),
                          ('Arlington', (38.878337, -77.100703))])
cities1.subtractByKey(cities2).collect()
cities2.subtractByKey(cities1).collect()    # Returns different values

# RDD.min(key=None)
numbers = sc.parallelize([0, 1, 1, 2, 3, 5, 8, 13, 21, 34])
numbers.min()

# RDD.max(key=None)
numbers = sc.parallelize([0, 1, 1, 2, 3, 5, 8, 13, 21, 34])
numbers.max()

# RDD.mean()
numbers = sc.parallelize([0, 1, 1, 2, 3, 5, 8, 13, 21, 34])
numbers.mean()

# RDD.sum()
numbers = sc.parallelize([0, 1, 1, 2, 3, 5, 8, 13, 21, 34])
numbers.sum()

# RDD.stdev()
numbers = sc.parallelize([0, 1, 1, 2, 3, 5, 8, 13, 21, 34])
numbers.stdev()

# RDD.variance()
numbers = sc.parallelize([0, 1, 1, 2, 3, 5, 8, 13, 21, 34])
numbers.variance()

# RDD.stats()
numbers = sc.parallelize([0, 1, 1, 2, 3, 5, 8, 13, 21, 34])
numbers.stats()