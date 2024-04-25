from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, when, col, sum, avg, desc,stddev

##Crear sparksession
spark = SparkSession.builder.appName("Test").getOrCreate()
df = spark.read.option("header", "true").option("inferSchema", "true").csv("F:/Alumno-105/PycharmProjects/MasterBDDS/natality.csv")


#Con el API del DataFrame

#1. #10 estadoscon mas nacimientos en 2003
df_2003 = df.filter(col("source_year") == 2003)
contarstate = df_2003.groupBy("state").count()
top_states = contarstate.orderBy(col("count").desc())
top_states.show(10)

#2. Obtén la media de peso de los bebes por año y estado.
df_filtrado = df.filter(col("state").isNotNull())
media = df_filtrado.groupBy("state", "year").agg(avg("weight_pounds").alias("Media"))
media.show()

#3. Evolucion por año y por mes del numero de niños y niñas nacidad (Resultado por separado con una sola consulta,
#cada registrodebe tener 4 columnas: año, mes, numero de niños nacidos, numero de niñas nacidas).

df_filtrado = df.filter(col("year").isNotNull() & col("month").isNotNull() & col("is_male").isNotNull())
df_etiquetado = df_filtrado.withColumn("genero", when(col("is_male"), "Niño_nacido").otherwise("Niña_nacida"))
nacimiento_por_genero = df_etiquetado.groupBy("year", "month").pivot("genero").count()
df_final = nacimiento_por_genero.orderBy("year", "month")
df_final.show()

# 4. Obten los tres meses de 2005 en que nacieron mas bebes

df_filtrado3= (df.filter(df.year == 2005))
nacidos_por_mes = df_filtrado3.groupBy("month").count()
topbebes_mes = nacidos_por_mes.withColumnRenamed("count", "totalmeses").orderBy(col("totalmeses").desc())
topbebes_mes.show(3)

#5. Obten los estados donde las semanas  de gestacion son superiores a la media de eeuu

df_media = df.select(avg("gestation_weeks").alias("mediaEEUU"))
mediaEEUU = df_media.collect()[0]["mediaEEUU"]
print(mediaEEUU)
df_filtrado = df.filter(col("state").isNotNull())
df_agrupado = df_filtrado.groupBy("state").agg(avg("gestation_weeks").alias("media_estado"))
df_superior = df_agrupado.filter(col("media_estado") > mediaEEUU)
df_superior.show()

#6. Obten 5 estados donde la media de edad de las madres ha sido mayor

df_filtrado = df.filter(col("state").isNotNull() & col("mother_age").isNotNull())
promedioedadmadre = df_filtrado.groupBy("state").agg(avg("mother_age").alias("promedioedadmadre"))
topestados = promedioedadmadre.orderBy(col("promedioedadmadre").desc()).limit(5)
topestados.show()

#7. Indica como influte en el peso del bebe y las semanas de gestacion que la madre haya tenido un parto multiple (campo plurality) a las que no lo han tenido.

gestation_by_plurality = df.groupBy("plurality").agg(avg("gestation_weeks").alias("avg_gestation_weeks")).orderBy("plurality")
gestation_by_plurality.show()

weight_by_plurality = df.groupBy("plurality").agg(avg("weight_pounds").alias("avg_weight_pounds")).orderBy("plurality")
weight_by_plurality.show()


## Con SQL
df.createOrReplaceTempView("natality")

#1. Obten 10 estados en que nacieron mas bebes en 2003
spark.sql("SELECT state, COUNT(*) AS contarstate FROM natality WHERE source_year = 2003 GROUP BY state ORDER BY contarstate DESC LIMIT 10").show()

#2. Obten la media de peso de los bebes poro año y estado
spark.sql("SELECT state, year, AVG(weight_pounds) AS Media FROM natality WHERE state IS NOT NULL GROUP BY state, year").show()

#3. Evolucion por año y por mes del numero de niños y niñas nacidas. (Resultado por separado con una sola consulta, cada registro debe tener 4 columnas: año, mes, numero de niños nacidos, numero de niñas nacidas
spark.sql("SELECT year, month, SUM(CASE WHEN is_male = true THEN 1 ELSE 0 END) AS ninosnacidos, SUM(CASE WHEN is_male = false THEN 1 ELSE 0 END) AS ninasnacidas  FROM natality GROUP BY year, month ORDER BY year, month").show()

#4.Obten los tres meses de 2005 en que nacieron mas bebes
spark.sql("SELECT month, COUNT(*) AS totalmeses FROM natality WHERE year = 2005 GROUP BY month ORDER BY totalmeses DESC LIMIT 3").show()

#5. Obten los estados donde las semanas  de gestacion son superiores a la media de eeuu
spark.sql("""
SELECT state, AVG(gestation_weeks) AS media_estado 
FROM natality 
WHERE state IS NOT NULL 
GROUP BY state 
HAVING AVG(gestation_weeks) > (SELECT AVG(gestation_weeks) FROM natality)
""").show()

#6
spark.sql("SELECT state, AVG(mother_age) AS avg_mother_age FROM natality WHERE state IS NOT NULL AND mother_age IS NOT NULL GROUP BY state ORDER BY avg_mother_age DESC LIMIT 5").show()

#7.
spark.sql("SELECT plurality, AVG(gestation_weeks) AS avg_gestation_weeks, AVG(weight_pounds) AS avg_weight_pounds FROM natality GROUP BY plurality ORDER BY plurality").show()
