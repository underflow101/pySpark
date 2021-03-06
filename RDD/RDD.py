# RDD.py
# Resilient Distributed Dataset

# Search error from log files

from pyspark import SparkContext

if sc is None: 
    sc = SparkContext(master="local", appName="first app")
else:
    sc.stop()
    sc = SparkContext(master="local", appName="first app")

# Load log files from local file system
logfilesrdd = sc.textFile("file:///var/log/hadoop/hdfs/hadoop-hdfs-*")
onlyerrorsrdd = logfilesrdd.filter(lambda line: "ERROR" in line)
onlyerrorsrdd.saveAsTextFile("file:///tmp/onlyerrorsrdd")
