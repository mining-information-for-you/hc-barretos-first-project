from pyspark import SparkContext, SparkConf, SparkFiles
from pyspark.sql import SQLContext

import os, sys
from loadConfig import load_config, load_source_files
from exam_done_io import exam_done_io
from exam_done_cassandraDB import exam_done_cassandraDB

def main():
    #Loading config file from json
    conf_file = load_config(sys.argv[1])

    source_files = conf_file["project"]["sourcePath"]
    csv_file = conf_file["csv-file"]["pathfilename"]
    keyspace = conf_file["cassandraDB"]["keyspace"]
    nodeIP = conf_file["cassandraDB"]["nodeIP"]

    sc = SparkContext()
    sql = SQLContext(sc)

    #Python files
    sc = load_source_files(sc, source_files)

    p_cassandra = exam_done_cassandraDB(nodeIP, keyspace)
    obj_exam_done_io = exam_done_io()

    obj_exam_done_io.exam_done_csv2Dataframe(sc,csv_file)
    obj_exam_done_io.get_dataframe_exam_done().show()
    #p_cassandra.insert_patient_benef(sql, p_io.get_dataframe_benef())

    p_cassandra.close_connection()


main()
