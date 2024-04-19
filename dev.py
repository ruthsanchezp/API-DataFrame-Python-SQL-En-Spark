from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, when, col, sum, avg, desc

##Crear sparksession
spark = SparkSession.builder.appName("Test").getOrCreate()
#print(spark.version)
df = spark.read.option("header", "true").option("inferSchema", "true").csv("F:/Alumno-105/PycharmProjects/MasterBDDS/natality.csv")
#df.printSchema()


#Con el API del DataFrame
#1.Obtén en que 10 estados nacieron más bebes en 2003
#estado = (df.filter(df.source_year == 2003)
 #         .select(
  #            "mother_residence_state",
   #           expr("coalesce(born_alive_alive, 0) + coalesce(born_alive_dead, 0) + coalesce(born_dead, 0)").alias("Suma"))
    #      .groupBy("mother_residence_state")
     #     .sum("Suma")
      #    .withColumnRenamed("sum(Suma)", "SumaTotal")
       #   .orderBy(col("SumaTotal").desc()))
#estado.show(10)

#o
##Aqui si considero el plurality
df_2003 = df.filter(col("source_year") == 2003)
df_2003 = df_2003.withColumn("total_births", col("plurality"))# Calcula el total de bebés nacidos por fila

# Agrupa por el estado y suma el número total de nacimientos
state_births = df_2003.groupBy("state").agg(sum("total_births").alias("total_births"))

# Ordena los resultados por el número total de nacimientos en orden descendente
top_states = state_births.orderBy(col("total_births").desc())

# Muestra los 10 primeros resultados

top_states.show(10)


#No alcanza un máximo de 10 estados en 2003, y CT tiene nacimientos sin data.

##Aqui no considero el plurality
df_2003 = df.filter(col("source_year") == 2003)

# Agrupar por estado y contar el número de filas para cada estado
state_birth_counts = df_2003.groupBy("state").count()

# Ordenar los resultados por el conteo en orden descendente
top_states = state_birth_counts.orderBy(col("count").desc())

# Mostrar los 10 estados con más nacimientos
top_states.show(10)

#Obtén la media de peso de los bebes por año y estado.
df_filtrado = df.filter(col("state").isNotNull())
media = df_filtrado.groupBy("state", "year").agg(avg("weight_pounds").alias("Media"))
media.show()



##Evolucion por año y por mes del numero de niños y niñas nacidad (Resultado por separado con una sola consulta,
#cada registrodebe tener 4 columnas: año, mes, numero de niños nacidos, numero de niñas nacidas).

df_filtrado = df.filter(col("year").isNotNull() & col("month").isNotNull() & col("is_male").isNotNull())
df_etiquetado = df_filtrado.withColumn("genero", when(col("is_male"), "Niño_nacido").otherwise("Niña_nacida"))
nacimiento_por_genero = df_etiquetado.groupBy("year", "month").pivot("genero").count()
df_final = nacimiento_por_genero.orderBy("year", "month")
df_final.show()

## Obten los tres meses de 2005 en que nacieron mas bebes

df_filtrado3= (df.filter(df.year == 2005))
nacidos_por_mes = df_filtrado3.groupBy("month").count()
topbebes_mes = nacidos_por_mes.withColumnRenamed("count", "totalmeses").orderBy(col("totalmeses").desc())
topbebes_mes.show(3)

#Obten los estados donde las semanas  de gestacion son superiores a la media de eeuu

df_media = df.select(avg("gestation_weeks").alias("mediaEEUU"))
mediaEEUU = df_media.collect()[0]["mediaEEUU"]
print(mediaEEUU)
df_filtrado = df.filter(col("state").isNotNull())
df_agrupado = df_filtrado.groupBy("state").agg(avg("gestation_weeks").alias("media_estado"))
df_superior = df_agrupado.filter(col("media_estado") > mediaEEUU)
df_superior.show()
