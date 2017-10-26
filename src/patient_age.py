
from pyspark import SparkContext, SparkConf, SparkFiles
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf,lit
from pyspark.sql.types import IntegerType, BooleanType


from datetime import datetime
import time

import os, sys
from loadConfig import load_config, load_source_files
from exam_done_cassandraDB import exam_done_cassandraDB

def data_validation(date):
    try:
        valid_date = time.strptime(date, '%m/%d/%Y')
        return True
    except ValueError:
        return False

def calcuate_age(birth_date, date):
    birth_date = datetime.strptime(birth_date, '%m/%d/%Y')
    date = datetime.strptime(date, '%m/%d/%Y')
    age = (date - birth_date).days/365
    return int(age)

def main():

    #Loading config file from json
    conf_file = load_config(sys.argv[1])

    source_files = conf_file["project"]["sourcePath"]
    keyspace = conf_file["cassandraDB"]["keyspace"]
    nodeIP = conf_file["cassandraDB"]["nodeIP"]

    sc = SparkContext()
    sqlCtx = SQLContext(sc)

    #Python files
    sc = load_source_files(sc, source_files)

    p_cassandra = exam_done_cassandraDB(nodeIP, keyspace)

    #Register user functions
    data_validation_udf = udf(data_validation, BooleanType())
    calcuate_age_udf = udf(calcuate_age, IntegerType())

    #Loading patient_done: codigo and dt_nascimento
    df_exam_done = p_cassandra.load_exam_done(sqlCtx)
    df_exam_done.registerTempTable("exam_done")

    # Adding data_validation collumn based on dt_nascimento
    df_exam_done = df_exam_done.withColumn('data_validation', data_validation_udf(df_exam_done.dt_nascimento))
    # Filter data_validation is True
    df_date_validation = df_exam_done.filter(df_exam_done["data_validation"] == lit(True))
    df_date_validation.show()

    # Criar um for em que os workers acrescentam de ano a ano e salvam no Cassandra
    # na tabela patient_age
    # No for criar a coluna date baseando-se na dt_nascimento e incrementar um ano.
    # (Utilizar a funcao do user)
    # No for criar a coluna year_ref e setar a interacao atual de year_ref por meio da funcao do usuario
    # inserir no Cassandra
main()
