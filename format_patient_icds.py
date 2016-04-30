from pyspark import SparkConf, SparkContext

import sys
import re


MIN_OCCURRENCES = 10
MAX_WORDS = 5000

out_file = 'hdfs:///user/steitzb/grad_students/outpatient_claims/*' 
in_file = 'hdfs:///user/steitzb/grad_students/inpatient_claims/*' 
pt_file = 'hdfs:///user/steitzb/grad_students/beneficiary/*'

save_outpt_file = 'hdfs:///user/steitzb/outpt_icd_sorted'
save_inpt_file = 'hdfs:///user/steitzb/inpt_icd_sorted'

'''
Format outpatient and inpatient claims data as (beneficiary_code | claim_end_date | ICD categories | outcome) 
'''
def get_specialties(spark, in_file, out_file, pt_file):
    pt_data = spark.textFile(pt_file) \
        .map(lambda line: line.split(",")) \
        .map(lambda line: (line[0], 1 if len(line[2]) > 4 else 0))

    out_data = spark.textFile(out_file) \
        .map(lambda line: line.split(",")) \
        .map(lambda line: (line[0], line[4], line[12], line[13],line[14], line[15], line[16], line[17], line[18], line[19], line[20], line[21])) \
        .keyBy(lambda line: line[0]) \
        .join(pt_data) \
        .map(lambda line: (line[1][0][0], line[1][0][1], line[1][0][2], line[1][0][3], line[1][0][4], line[1][0][5],
        line[1][0][6], line[1][0][7], line[1][0][8], line[1][0][9], line[1][0][10], line[1][0][11], line[1][1])) \
        .distinct() \
        .map(lambda line: (line[0], line[1][:6], line[2][:3], line[3][:3], line[4][:3], line[5][:3], line[6][:3],
        line[7][:3], line[8][:3], line[9][:3], line[10][:3], line[11][:3], line[12])) \
        .sortBy(lambda line: line[0])
    
    in_data = spark.textFile(in_file) \
        .map(lambda line: line.split(",")) \
        .map(lambda line: (line[0], line[4], line[20], line[21], line[22], line[23], line[24], line[25], line[26], line[27], line[28], line[29])) \
        .keyBy(lambda line: line[0]) \
        .join(pt_data) \
        .map(lambda line: (line[1][0][0], line[1][0][1], line[1][0][2], line[1][0][3], line[1][0][4], line[1][0][5],
        line[1][0][6], line[1][0][7], line[1][0][8], line[1][0][9], line[1][0][10], line[1][0][11], line[1][1])) \
         .distinct() \
         .map(lambda line: (line[0], line[1][:6], line[2][:3], line[3][:3], line[4][:3], line[5][:3], line[6][:3],
        line[7][:3], line[8][:3], line[9][:3], line[10][:3], line[11][:3], line[12])) \
        .sortBy(lambda line: line[0])
                                        
    #Output as .csv format
    out_data.map(writeCSV) \
        .saveAsTextFile(save_outpt_file)
    in_data.map(writeCSV) \
        .saveAsTextFile(save_inpt_file)

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
