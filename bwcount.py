from pyspark import SparkContext
import sys

if __name__=='__main__':
    sc = SparkContext()
    sc.textFile(sys.argv[1] if len(sys.argv)>1 else 'mybook.txt') \
    	.take(20)\
        .flatMap(lambda x: x.split()) \
        .map(lambda x: (x,1)) \
        .reduceByKey(lambda x,y: x+y) \
        .saveAsTextFile(sys.argv[2] if len(sys.argv)>2 else 'out2')