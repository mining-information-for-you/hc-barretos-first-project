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
    sqlCtx = SQLContext(sc)

    #Python files
    sc = load_source_files(sc, source_files)

    p_cassandra = exam_done_cassandraDB(nodeIP, keyspace)

    df_exam_done = p_cassandra.load_exam_done(sqlCtx)
    df_exam_done.createOrReplaceTempView("exam_done")
    #Show all exam_done
    sqlCtx.sql("select * from exam_done").show()

    p_cassandra.close_connection()


main()
