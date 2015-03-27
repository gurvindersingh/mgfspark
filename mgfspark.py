from pyspark import SparkContext
from pyspark.conf import SparkConf
from utils import parseMGF

conf = SparkConf()
conf.setAppName("MGF test").set("spark.executor.memory", "10g")
sc = SparkContext(conf=conf)
path = "hdfs://daas/promec/mgf"


def process(raw_data):
    mgf_data = raw_data.map(lambda x: parseMGF(x[1]))
    return mgf_data


raw_data = sc.wholeTextFiles(path, use_unicode=False, minPartitions=40)
mgf_data = process(raw_data)
# mgf_data.repartition(40)
print mgf_data.take(1)
print mgf_data.count()
#print(raw_data.count())