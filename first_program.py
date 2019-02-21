from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("First Spark App")
sc = SparkContext(conf=conf)

sc.setLogLevel("ERROR")

random_data = sc.textFile('<SPARK_HOME>/data/mllib/lr-data/random.data')
print random_data.count()
print random_data.first()

start_with_zero = random_data.filter(lambda x: x.startswith('0.0,'))
print start_with_zero.count()
