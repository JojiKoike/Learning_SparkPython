from pyspark import SparkConf, SparkContext
import sys

args = sys.argv
conf: SparkConf = SparkConf().setMaster("local").setAppName("RDD Conversion")
sc: SparkContext = SparkContext(conf=conf)
input_file = sc.textFile(args[1])
words = input_file.filter(lambda line: "POST" in line)
words.saveAsTextFile(args[2])
