from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv("./csv/ministerios-publicos-actos-procesales-2021-semestre-1.csv", header=True)
remove_column = (
        df.select(
                "provincia_nombre", "delito_descripcion",
                "caso_fecha_inicio","autor_genero",
                "autor_edad"
        )
)


clean_df = (remove_column
        .withColumn("delito_descripcion", regexp_replace("delito_descripcion", r"^Hurto.*", "Hurto"))
        .withColumn("delito_descripcion", regexp_replace("delito_descripcion", r"^Robo.*", "Robo"))
        .withColumn("delito_descripcion", regexp_replace("delito_descripcion", r"^Abuso.sexual.*", "Abuso sexual"))
        .withColumn("delito_descripcion", regexp_replace("delito_descripcion", r"^Lesiones.leves.*", "Lesiones leves"))
        .withColumn("delito_descripcion", regexp_replace("delito_descripcion", r"^Amenazas.*", "Amenazas"))
        .withColumn("delito_descripcion", regexp_replace("delito_descripcion", r"^Homicidio.*", "Homicidio"))
        .withColumn("delito_descripcion", regexp_replace("delito_descripcion", r"^Coacciones.*", "Coacciones"))
        .withColumn("delito_descripcion", regexp_replace("delito_descripcion", r"^Estafa.*", "Estafa"))
        .withColumn("delito_descripcion", regexp_replace("delito_descripcion", r"^Encubrimiento.*", "Encubrimiento"))
        .withColumn("delito_descripcion", regexp_replace("delito_descripcion", r"^Violación.de.medidas.contra.epidemias.*", "Violación de medidas contra epidemias"))
        .withColumn("delito_descripcion", regexp_replace("delito_descripcion", r"^Lesiones.culposas.*", "Lesiones culposas graves o gravísimas"))
        .withColumn("delito_descripcion", regexp_replace("delito_descripcion", r"^Desobediencia.a.la.autoridad.*", "Desobediencia a la autoridad"))
        .withColumn("delito_descripcion", regexp_replace("delito_descripcion", r"^Daños de una cosa mueble o inmueble o un animal.*", "Daños de una cosa mueble o inmueble o un animal"))
        .withColumn("delito_descripcion", regexp_replace("delito_descripcion", r"^Resistencia.o.desobediencia.a.un.funcionario.público.*", "Resistencia o desobediencia a un funcionario público"))
        .withColumn("delito_descripcion", regexp_replace("delito_descripcion", r"^Instigación.o.ayuda.al.suicidio.*", "Instigación o ayuda al suicidio"))
        .withColumn("delito_descripcion", regexp_replace("delito_descripcion", r"^Violación.de.domicilio.*", "Violación de domicilio"))
        .withColumn("delito_descripcion", regexp_replace("delito_descripcion", r"^\"?Usurpación.*", "Usurpación"))
        .withColumn("delito_descripcion", regexp_replace("delito_descripcion", r"^Daños.+.Art\..183.*", "Daños"))
        .withColumn("delito_descripcion", regexp_replace("delito_descripcion", r"^Defraudación.+Art. 173*", "Defraudación"))
        .withColumn("delito_descripcion", regexp_replace("delito_descripcion", r"^\d+$", ""))
        .withColumn("delito_descripcion", regexp_replace("delito_descripcion", r"^#N/A.*", ""))
        .withColumn("delito_descripcion", regexp_replace("delito_descripcion", r"^A.caratular.*", ""))
        .filter(col("delito_descripcion") != "")
)

grouping_df = clean_df.groupBy("delito_descripcion").count().orderBy("count", ascending=False)

grouping_df.coalesce(1).write.format("com.databricks.spark.csv").save("spark")