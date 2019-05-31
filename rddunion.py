from pyspark import SparkConf, SparkContext
import sys

args = sys.argv
conf: SparkConf = SparkConf().setMaster("local").setAppName("RDD Union")
sc: SparkContext = SparkContext(conf=conf)
input_file = sc.textFile(args[1])
rdd_post = input_file.filter(lambda line: "POST" in line)
rdd_get = input_file.filter(lambda line: "GET" in line)
rdd_post.union(rdd_get).saveAsTextFile(args[2])
