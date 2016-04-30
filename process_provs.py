from pyspark import SparkConf, SparkContext

import sys
import re


MIN_OCCURRENCES = 10
MAX_WORDS = 5000

out_file = 'hdfs:///user/steitzb/grad_students/outpatient_claims/*' 
in_file = 'hdfs:///user/steitzb/grad_students/inpatient_claims/*' 
pt_file = 'hdfs:///user/steitzb/grad_students/beneficiary/*'

save_file = 'hdfs:///user/steitzb/prov_counts'

'''
Function to get patient, and provider specialties associated with care 
'''
def get_specialties(spark, in_file, out_file, pt_file):
    patients = spark.textFile(pt_file) \
        .map(lambda line: line.split(",")) \
        .map(lambda line: (line[0], line[2]))

    out_data = spark.textFile(out_file) \
        .map(lambda line: line.split(",")) \
        .map(lambda line: (line[0], line[4], line[8])) \
        .map(lambda(code, date, phys): ((code, date[:6]), 1)) \
        .reduceByKey(lambda x, y: x + y)
    in_data = spark.textFile(in_file) \
        .map(lambda line: line.split(",")) \
        .map(lambda line: (line[0], line[4], line[8])) \
        .map(lambda(code, date, phys): ((code, date[:6]), 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .keyBy(lambda(key, val): key) \
        .join(out_data) \
        .map(lambda line: (line[1][0][0][0], line[1][0][0][1], int(line[1][0][1]) + int(line[1][1]))) \
        .keyBy(lambda(patient, date, count): patient) \
        .join(patients) \
        .map(lambda line: (line[1][0][0], line[1][0][1], line [1][0][2], line[1][1])) \
        .distinct() \
        .sortBy(lambda line: line[0])
    final_output = in_data.map(writeCSV) \
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
    get_specialties(spark, in_file, out_file, pt_file)
