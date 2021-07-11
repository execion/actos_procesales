from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from etl.rename_rows import rename_rows
from etl.load_db import load_in_db
from config import *

spark = SparkSession.builder.getOrCreate()

# ---------------------------------------      EXTRACT STAGE      ---------------------------------------

df = spark.read.csv("./csv/*.csv", header=True) # Read the csvs from 2013 to 2021. All files have 1,1GB aproximately.

select_columns = (
	df.select(
		"provincia_nombre", 
		"delito_descripcion",
		"caso_fecha_inicio",
		"autor_genero",
	)
)

# ---------------------------------------      TRANSFORM STAGE       ---------------------------------------

# It's all regular expression for rename each row in the column "delito_descripcion"
crime_descrip_regx = [
	[r"^Hurto.*", "Hurto"], [r"^Robo.*", "Robo"],
	[r"^Abuso.sexual.*", "Abuso sexual"], [r"^Lesiones.leves.*", "Lesiones leves"],
	[r"^Amenazas.*", "Amenazas"], [ r"^Homicidio.*", "Homicidio"],
	[r"^Homicidio.*", "Homicidio"], [r"^Coacciones.*", "Coacciones"],
	[r"^Estafa.*", "Estafa"], [r"^Encubrimiento.*", "Encubrimiento"],
	[r"^Violación.de.medidas.contra.epidemias.*", "Violación de medidas contra epidemias"],
	[r"^Lesiones.culposas.*", "Lesiones culposas graves o gravísimas"],
	[r"^Desobediencia.a.la.autoridad.*", "Desobediencia a la autoridad"],
	[r"^Daños de una cosa mueble o inmueble o un animal.*", "Daños de una cosa mueble o inmueble o un animal"],
	[r"^Resistencia.o.desobediencia.a.un.funcionario.público.*", "Resistencia o desobediencia a un funcionario público"],
	[r"^Instigación.o.ayuda.al.suicidio.*", "Instigación o ayuda al suicidio"],
	[r"^Violación.de.domicilio.*", "Violación de domicilio"],
	[r"^\"?Usurpación.*", "Usurpación"],
	[r"^Daños.+.Art\..183.*", "Daños"],
	[r"^Defraudación.+Art. 173*", "Defraudación"],
	[r"^\d+$", ""], [r"^#N/A.*", ""],
	[r"^A.caratular.*", ""]
]

#Rename the rows. Especially the rows that contain Articles or don't have a valid value.
rename_crime_description = rename_rows(dataframe=select_columns, column="delito_descripcion", regxs=crime_descrip_regx) 

#Filter the void area because in the rename_rows function the are regular expression that change to "" in base some criteria
filter_voids = rename_crime_description.filter(
		(col("delito_descripcion") != "") & 
		(col("caso_fecha_inicio") != "") & 
		((col("autor_genero") == "F") | (col("autor_genero") == "M")) & 
		(col("provincia_nombre") != "")
	)

# ---------------------------------------      LOAD STAGE      ---------------------------------------

filter_voids.foreach(lambda x: load_in_db(x, db)) #Load in Database. The compressed file contain 65,5MB around.