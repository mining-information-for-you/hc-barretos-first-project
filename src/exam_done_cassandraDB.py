from pyspark.sql import SQLContext, Row

from commonCassandraDB import CommonCassandraDB

class exam_done_cassandraDB(CommonCassandraDB):

    def __init__(self, nodeIP, keyspace):
        self.set_cassandra_client(nodeIP, keyspace)

    def insert_patient_benef(self, sqlCtx, df):
        table = self.client.get_keyspace() + "." + "patient"
        sql_query = """
        INSERT INTO @$$$$$$$$$$$$@
        (id_ness_patient, cod_progenitor, progenitor_is_benef, birthday, gender, city, state, skin_color, blood_type)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        sql_query = sql_query.replace("@$$$$$$$$$$$$@", table)
        bound_statement = self.client.session.prepare(sql_query)

        for row in df.rdd.collect():
            #print(row.COD_BEN)
            self.client.session.execute( bound_statement.bind((
            uuid1(),
            row.COD_BEN,
            row.is_benef,
            row.DTNASC,
            row.SEXO,
            row.CIDADE,
            row.ESTADO,
            row.COR,
            row.TP_SANGUE
            ))
        )

    def insert_patient_depend(self, sqlCtx, df):
        table = self.client.get_keyspace() + "." + "patient"
        sql_query = """
        INSERT INTO @$$$$$$$$$$$$@
        (id_ness_patient, cod_progenitor, progenitor_is_benef, birthday, gender, city, state, skin_color, blood_type)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        sql_query = sql_query.replace("@$$$$$$$$$$$$@", table)
        bound_statement = self.client.session.prepare(sql_query)

        for row in df.rdd.collect():
            self.client.session.execute( bound_statement.bind((
            uuid1(),
            row.COD_BEN,
            row.is_benef,
            row.DTNASC,
            row.SEXO,
            row.CIDADE,
            row.ESTADO,
            row.COR,
            row.TP_SANGUE
            ))
        )
