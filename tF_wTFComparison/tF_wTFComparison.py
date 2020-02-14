# Comparison between textFile() and wholeTextFiles()

from pyspark import SparkContext

if sc is None: 
    sc = SparkContext(master="local", appName="first app")
else:
    sc.stop()
    sc = SparkContext(master="local", appName="first app")
    
licenseFiles = sc.textFile("file:///opt/spark/licenses/")
licenseFiles
licenseFiles.take(1)
licenseFiles.getNumPartitions()
licenseFiles.count()

licenseFilePairs = sc.wholeTextFiles("file:///opt/spark/licenses")
licenseFilePairs
licenseFilePairs.take(1)
licenseFilePairs.getNumPartitions()
licenseFilePairs.count()