#!/usr/bin/env python

'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''

import sys
from pyspark import SparkConf, SparkContext

stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]

stopWordsList = [];

with open(stopWordsPath) as f:
	#TODO
    for line in f:
        stopWordsList.append(line.strip())

with open(delimitersPath) as f:
    #TODO
    delimiters = f.read()

conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[3], 1)

#TODO
def preprocess(line):
    for d in delimiters:
        line = line.strip().replace(d, " ").lower()
    return line

words = lines.map(preprocess).flatMap(lambda line: line.split()) # to filter by stopWords .filter(lambda word: word not in stopWordsList)

counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

sorted_counts = sorted(counts, key=lambda word_count: word_count[1], reverse=True)

top_counts = sorted(sorted_counts[:9], key=lambda word_count: word_count[0])

outputFile = open(sys.argv[4],"w")

#TODO
#write results to output file. Foramt for each line: (line +"\n")
for tc in top_counts:
    f.write("%s\t%s\n" % tc)

sc.stop()
