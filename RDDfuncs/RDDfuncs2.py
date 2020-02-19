# Spark RDD functions 2

import sys, re
from pyspark import SparkConf, SparkContext, RDD

conf = SparkConf().setAppName('RDD Func')
sc = SparkContext(conf=conf)

############################################################
# station-id:       station ID number
# name:             station name
# lat:              latitude
# long:             longitude
# dockcount:        number of docks which embeds station
# landmark:         city
# installation:     date station was embedded
# bikes_available:  number of available bicycle
# docks_available:  number of available docks
# time:             time and date, PST
############################################################
stations = sc.textFile('/opt/spark/data/bike-share/stations')
status = sc.textFile('/opt/spark/data/bike-share/status')

status2 = status.map(lambda x: x.split(',')) \
                .map(lambda x: (x[0], x[1], x[2], x[3].replace('"', ''))) \
                .map(lambda x: (x[0], x[1], x[2], x[3].split(' '))) \
                .map(lambda x: (int(x[0]), int(x[1]), int(x[3][0]), int(x[3][1]), int(x[3][2]), int(x[4][0])))
status2.first()

status3 = status2.filter(lambda x: x[2] == 2015 and x[3] == 2 and x[4] >= 22) \
                 .map(lambda x: (x[0], x[1], x[5]))

stations2 = stations.map(lambda x: x.split(',')) \
                    .filter(lambda x: x[5] == 'San Jose') \
                    .map(lambda x: (int(x[0]), x[1]))

stations2.first()

status_kv = status3.keyBy(lambda x: x[0])
stations_kv = stations2.keyBy(lambda x: x[0])

status_kv.first()
stations_kv.first()

joined = status_kv.join(stations_kv)
joined.first()

cleaned = joined.map(lambda x: (x[0], x[1][0][1], x[1][0][2], x[1][1][1]))
cleaned.first()

avgByHour = cleaned.keyBy(lambda x: (x[3], x[2])) \
                   .mapValues(lambda x: (x[1], 1)) \
                   .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
                   .mapValues(lambda x: (x[0] / x[1]))                   
avgByHour.first()

topAvail = avgByHour.keyBy(lambda x: x[1]) \
                    .sortByKey(ascending=False) \
                    .map(lambda x: (x[1][0][0], x[1][0][1], x[0]))
topAvail.take(10)