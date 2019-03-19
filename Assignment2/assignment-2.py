from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Word Count App")
sc = SparkContext(conf=conf)

# read text file
txt = sc.textFile("assignment_2_datafile.txt")

# create flatmap of words
words = txt.flatMap(lambda line: line.split(" "))

# filter out words with less than 3 letters
words_filt = words.filter(lambda x: len(x) > 2)

# convert words to lowercase
words_filt2 = words_filt.map(lambda x: x.lower())

# filter out numbers
words_filt3 = words_filt2.filter(lambda x: x.isalpha())

# filter out 'the'
words_filt4 = words_filt3.filter(lambda x: x != "the")

# print results
print("Number of words = " + str(words_filt4.count()))
