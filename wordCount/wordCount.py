# Word Count with Spark

import sys, re
from pyspark import SparkConf, SparkContext, RDD

conf = SparkConf().setAppName('Word Counts')
sc = SparkContext(conf=conf)

if(len(sys.argv) != 3):
    print("""\
This Program will count occurrences of each word in a document or documents
and return the counts sorted by the most frequently occurring words

Usage: wordCount.py <input_file_or_dir> <output_dir>
""")
    sys.exit(0)
else:
    inputPath = sys.argv[1]
    outputDir = sys.argv[2]
    
wordCounts = sc.textFile("file://" + inputPath) \
                .filter(lambda line: len(line) > 0) \
                .flatMap(lambda line: re.split('\W+', line)) \
                .filter(lambda word: len(word) > 0) \
                .map(lambda word: (word.lower(), 1)) \
                .reduceByKey(lambda v1, v2: v1 + v2) \
                .map(lambda x: (x[1], x[0])) \
                .sortByKey(ascending=False) \
                .persist()

wordCounts.saveAsTextFile("file://" + outputDir)
topFiveWords = wordCounts.take(5)

justWords = list()

for wordSandCounts in topFiveWords:
    justWords.append(wordSandCounts[1])

print("Top 5 words are: " + str(justWords))
print("check the complete output in " + outputDir)