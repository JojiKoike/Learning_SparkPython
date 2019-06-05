from pyspark import SparkConf, SparkContext
import sys

args = sys.argv
conf: SparkConf = SparkConf().setMaster("local").setAppName("RDDAction")
sc: SparkContext = SparkContext(conf=conf)
input_file = sc.textFile(args[1])
for line in input_file.filter(lambda line: "POST" in line).take(10):
    print(line)