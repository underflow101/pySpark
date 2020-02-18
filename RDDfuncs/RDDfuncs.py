# Spark RDD functions

import sys, re
from pyspark import SparkConf, SparkContext, RDD

conf = SparkConf().setAppName('RDD Func')
sc = SparkContext(conf=conf)

stores = sc.parallelize([(100, 'Boca Raton'),
                         (101, 'Columbia'),
                         (102, 'Cambridge'),
                         (103, 'Naperville')])

# stores schema
salespeople = sc.parallelize([(1, 'Henry', 100),
                              (2, 'Karen', 100),
                              (3, 'Paul', 101),
                              (4, 'Jimmy', 102),
                              (5, 'Janice', None)])

# RDD.join(<otherRDD>, numPartitions=None)
salespeople.keyBy(lambda x: x[2]).join(stores).collect()

# RDD.leftOuterJoin(<otherRDD>, numPartitions=None)
salespeople.keyBy(lambda x: x[2]).leftOuterJoin(stores) \
                                 .filter(lambda x: x[1][1] is None) \
                                 .map(lambda x: "salesperson" + x[1][0][1] + " has no store").collect()

# RDD.rightOuterJoin(<otherRDD>, numPartitions=None)
salespeople.keyBy(lambda x: x[2]).rightOuterJoin(stores) \
                                 .filter(lambda x: x[1][0] is None) \
                                 .map(lambda x: x[1][1] + " store has no salespeople").collect()

# RDD.fullOuterJoin(<otherRDD>, numPartitions=None)
salespeople.keyBy(lambda x: x[2]).fullOuterJoin(stores) \
                                 .filter(lambda x: x[1][0] is None or x[1][1] is None).collect()

# RDD.cogroup(<otherRDD>, numPartitions=None)
salespeople.keyBy(lambda x: x[2]).cogroup(stores).take(1)
salespeople.keyBy(lambda x: x[2]).cogroup(stores) \
                                 .mapValues(lambda x: [item for sublist in x for item in sublist]).collect()

# RDD.catesian(<otherRDD>)
salespeople.keyBy(lambda x: x[2]).cartesian(stores).take(1)
salespeople.keyBy(lambda x: x[2]).cartesian(stores).count()