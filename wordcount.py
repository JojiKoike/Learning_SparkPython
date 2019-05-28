from pyspark import SparkConf, SparkContext
import sys

args = sys.argv
conf: SparkConf = SparkConf().setMaster("local").setAppName("My App")
sc: SparkContext = SparkContext(conf=conf)
input_file = sc.textFile(args[1])
words = input_file.flatMap(lambda line: line.split(" "))
counts = words.map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y)
counts.saveAsTextFile(args[2])
