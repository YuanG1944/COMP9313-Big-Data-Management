from pyspark import SparkContext, SparkConf
from time import time
import pickle
import submission
import version1
import version2
import version3

def createSC():
    conf = SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("C2LSH")
    sc = SparkContext(conf = conf)
    return sc

# with open("/Users/yuan/9313 Big Data Management/project1/COMP9313_project_1_v1/toy/toy_hashed_data", "rb") as file:
#     data = pickle.load(file)

# with open("/Users/yuan/9313 Big Data Management/project1/COMP9313_project_1_v1/toy/toy_hashed_query", "rb") as file:
#     query_hashes = pickle.load(file)

with open("/Users/yuan/9313 Big Data Management/project1/COMP9313_project_1_v1/testCase30w.pkl", "rb") as file:
    data = pickle.load(file)

with open("/Users/yuan/9313 Big Data Management/project1/COMP9313_project_1_v1/testQueryHash30w.pkl", "rb") as file:
    query_hashes = pickle.load(file)

# with open("/Users/yuan/9313 Big Data Management/project1/COMP9313_project_1_v1/testCase100w.pkl", "rb") as file:
#     data = pickle.load(file)

# with open("/Users/yuan/9313 Big Data Management/project1/COMP9313_project_1_v1/testQueryHash100w.pkl", "rb") as file:
#     query_hashes = pickle.load(file)

# with open("/Users/yuan/9313 Big Data Management/project1/COMP9313_project_1_v1/testCase100w_32.pkl", "rb") as file:
#     data = pickle.load(file)

# with open("/Users/yuan/9313 Big Data Management/project1/COMP9313_project_1_v1/testQueryHash100w_32.pkl", "rb") as file:
#     query_hashes = pickle.load(file)

# with open("/Users/yuan/9313 Big Data Management/project1/COMP9313_project_1_v1/testCaseSp10w.pkl", "rb") as file:
#     data = pickle.load(file)

# with open("/Users/yuan/9313 Big Data Management/project1/COMP9313_project_1_v1/testQueryHash.pkl", "rb") as file:
#     query_hashes = pickle.load(file)

alpha_m  = 10
beta_n = 10

sc = createSC()
data_hashes = sc.parallelize([(index, x) for index, x in enumerate(data)])
# print(data_hashes.collect())
# print(data_hashes.count())
# start_time = time()
# res = submission.c2lsh(data_hashes, query_hashes, alpha_m, beta_n).collect()
# res = version1.c2lsh(data_hashes, query_hashes, alpha_m, beta_n).collect()
# res = version2.c2lsh(data_hashes, query_hashes, alpha_m, beta_n).collect()
res = version3.c2lsh(data_hashes, query_hashes, alpha_m, beta_n)
print(res)
sc.stop()

# print('set of candidate: ', set(res))