from pyspark import SparkContext
from pyspark.conf import SparkConf
from utils import parseMGF
import argparse

DESCRIPTION = "MGF data analysis using Apache Spark"


def parse_args():
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument('path', help="Directory path to MGF files")
    parser.add_argument('-p', '--partitions', default="60", type=int,
                        help="Number of partitions to be used for processing")

    return parser.parse_args()


def process(raw_data):
    mgf_data = raw_data.map(lambda x: parseMGF(x[1]))
    return mgf_data


if __name__ == '__main__':
    args = parse_args()
    conf = SparkConf()
    conf.setAppName("MGF test").set("spark.executor.memory", "10g")
    sc = SparkContext(conf=conf)
    raw_data = sc.wholeTextFiles(args.path, use_unicode=False, minPartitions=args.partitions)
    mgf_data = process(raw_data).repartition(args.partitions)
    print mgf_data.take(1)
    # print mgf_data.take(1)[0]['params']
    print mgf_data.count()
