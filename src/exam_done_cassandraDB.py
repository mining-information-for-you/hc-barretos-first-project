from pyspark.sql import SQLContext, Row

from commonCassandraDB import CommonCassandraDB

class exam_done_cassandraDB(CommonCassandraDB):

    def __init__(self, nodeIP, keyspace):
        self.set_cassandra_client(nodeIP, keyspace)

    def load_exam_done(self, sqlCtx):
        return sqlCtx.read.format("org.apache.spark.sql.cassandra").options(table="exame_done", keyspace=self.client.get_keyspace()).load()

    def insert_exam_done(self, sqlCtx, df):
        table = self.client.get_keyspace() + "." + "exame_done"
        sql_query = """
        INSERT INTO @$$$$$$$$$$$$@
        (PES_ID_PACIENTE,DT_NASCIMENTO,SEXO,ESTADO_CIVIL,RACA_COR,GRAU_INSTRUCAO,PROFISSAO,NACIONALIDADE,NATURALIDADE,CIDADE,UF,BAIRRO,DATA_EXAME,EXAME,CENTRO_CUSTO)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
        """
        sql_query = sql_query.replace("@$$$$$$$$$$$$@", table)
        bound_statement = self.client.session.prepare(sql_query)

        for row in df.rdd.collect():
            self.client.session.execute( bound_statement.bind((
                int(row.PES_ID_PACIENTE),
                row.DT_NASCIMENTO,
                row.SEXO,
                row.ESTADO_CIVIL,
                row.RACA_COR,
                row.GRAU_INSTRUCAO,
                row.PROFISSAO,
                row.NACIONALIDADE,
                row.NATURALIDADE,
                row.CIDADE,
                row.UF,
                row.BAIRRO,
                row.DATA_EXAME,
                row.EXAME,
                row.CENTRO_CUSTO
            ))
        )
