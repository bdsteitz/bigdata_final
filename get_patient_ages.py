from pyspark import SparkConf, SparkContext

import sys
import re


MIN_OCCURRENCES = 10
MAX_WORDS = 5000

pt_file = 'hdfs:///user/steitzb/grad_students/beneficiary/*'
save_file = 'hdfs:///user/steitzb/patient_ages'

'''
Function to get patient age based on beneficiary file
Output: beneficiary_id | age | outcome
'''
def get_icd(spark, pt_file):
    patients = spark.textFile(pt_file) \
        .map(lambda line: line.split(",")) \
        .map(lambda line: (line[0], line[1], line[2])) \
        .map(writeCSV) \
        .saveAsTextFile(save_file)


def writeCSV(data):
    return ','.join(str(d) for d in data)

if __name__ == '__main__':
    conf = SparkConf()
    if sys.argv[1] == 'local':
        conf.setMaster("local[3]")
        print 'Running locally'
    elif sys.argv[1] == 'cluster':
        conf.setMaster("spark://10.0.22.241:7077")
        print 'Running on cluster'
    conf.set("spark.executor.memory", "10g")
    conf.set("spark.driver.memory", "10g")
    spark = SparkContext(conf = conf)
    get_icd(spark, pt_file)
