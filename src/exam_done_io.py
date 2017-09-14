from pyspark.sql import Row


class exam_done_io():

    def __init__(self):
        self.df_exam_done = None

    def exam_done_csv2Dataframe(self,sc,csvfile):
        csv_RDD = sc.textFile(csvfile)
        header = csv_RDD.first() #extract header
        csv_RDD = csv_RDD.filter(lambda x:x !=header).map(lambda line: line)
        csv_RDD = csv_RDD.map(lambda line: line.split(",") )
        csv_RDD = csv_RDD.map(lambda p: Row(PES_ID_PACIENTE=self.prepare_field_str(str(p[0])), DT_NASCIMENTO=self.prepare_field_str(str(p[1])), SEXO=self.prepare_field_str(str(p[2])), ESTADO_CIVIL=self.prepare_field_str(str(p[3])), RACA_COR=self.prepare_field_str(str(p[4])), GRAU_INSTRUCAO=self.prepare_field_str(str(p[5])), PROFISSAO=self.prepare_field_str(str(p[6])), NACIONALIDADE=self.prepare_field_str(str(p[7])),NATURALIDADE=self.prepare_field_str(str(p[8])),CIDADE=self.prepare_field_str(str(p[8])),UF=self.prepare_field_str(str(p[9])),BAIRRO=self.prepare_field_str(str(p[10])),DATA_EXAME=self.prepare_field_str(str(p[11])),EXAME=self.prepare_field_str(str(p[12])),CENTRO_CUSTO=self.prepare_field_str(str(p[13])) ) )
        self.df_exam_done = csv_RDD.toDF()

    def prepare_field_str(self, field):
        #Remove spaces
        field = field.strip()
        return field

    def get_dataframe_exam_done(self):
        return self.df_exam_done
