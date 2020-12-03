from pyspark import SparkConf, SparkContext
 
# create SparkConf and SparkContext
conf = SparkConf().setMaster("local").setAppName("hellow_9313")
sc = SparkContext(conf = conf)

raw_data = [("Joseph", "Maths", 83), ("Joseph", "Physics", 74), ("Joseph", "Chemistry", 91), ("Joseph", "Biology", 82), ("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62),
("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80), ("Tina", "Maths", 78), ("Tina", "Physics", 73),
    ("Tina", "Chemistry", 68), ("Tina", "Biology", 87),
    ("Thomas", "Maths", 87), ("Thomas", "Physics", 93),
    ("Thomas", "Chemistry", 91), ("Thomas", "Biology", 74)]

# rdd_1 = sc.parallelize(raw_data)
# rdd_2 = rdd_1.map(lambda x:(x[0], x[2]))
# rdd_3 = rdd_2.reduceByKey(lambda x, y:max(x, y)) 
# rdd_4 = rdd_2.reduceByKey(lambda x, y:min(x, y)) 
# rdd_5 = rdd_3.join(rdd_4)
# rdd_6 = rdd_5.map(lambda x: (x[0], x[1][0]+x[1][1])) 
# rdd_6.collect()

rdd_1 = sc.parallelize(raw_data)
rdd_2 = rdd_1.map(lambda x:(x[0], x[2]))
rdd_3 = rdd_2.groupByKey() 
rdd_4 = rdd_3.mapValues(lambda x:max(x) + min(x)) 
print(rdd_4.collect())