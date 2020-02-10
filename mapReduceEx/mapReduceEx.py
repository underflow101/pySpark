# PySpark Programming
# Map Reduce Example

from pyspark import SparkContext
import time
import matplotlib.pyplot as plt
import numpy as np 
import pandas as pd

if sc is None: 
    sc = SparkContext(master="local", appName="first app")
else:
    sc.stop()
    sc = SparkContext(master="local", appName="first app")

lines = sc.textFile("file:///opt/spark/licenses")
counts = lines.flatMap(lambda x: x.split(' '))
filter(lambda x: len(x) > 0)
map(lambda x: (x, 1))
reduceByKey(lambda x, y: x + y)
collect()

for (word, count) in counts:
    print("%s: %i" % (word, count))