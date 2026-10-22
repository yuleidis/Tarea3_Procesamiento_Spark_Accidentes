from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("AnalisisAccidentes").getOrCreate()
ruta = "hdfs://localhost:9000/user/vboxuser/vehiculos_accidentes/vehiculos_accidentes.csv"

df = spark.read \
     .option("header", "true")\
     .option("inferSchema", "true") \
     .csv(ruta)
print("\n===== DATOS ORIGINALES =====")
df.printSchema()


df = df.dropDuplicates()

df = df.dropna(

     subset=[
         "marca_vehiculo",
         "tipo_vehiculo",
         "edad_vehiculo",
         "gravedad_accidente"
      ]

)

df = df.withColumn (
      "edad_vehiculo",
       F.col("edad_vehiculo").cast("integer")
)

df = df.filter(F.col("edad_vehiculo") >= 0)

print("\n===== DATOS DESPUÈS DE LA LIMPIEZA =====")
df.show(10, truncate=False)

print("\n===== CANTIDAD DE REGISTROS =====")
print(df.count())


print("\n===== VEHÌCULOS POR TIPOS =====")
df.groupBy("tipo_vehiculo") \
     .count() \
     .orderBy(F.desc("count")) \
     .show(10, truncate=False)


print("\n====== ACCIDENTE POR GRAVEDAD =====")
df.groupBy("gravedad_accidente") \
     .count() \
     .orderBy(F.desc("count")) \
     .show(truncate=False)

print("\n====== EDAD PROMEDIO POR TIPO DE VEHICULO =====")
df.groupBy("tipo_vehiculo") \
   .agg(F.round(F.avg("edad_vehiculo"), 2).alias("edad_promedio")) \
   .orderBy(F.desc("edad_promedio")) \
   .show(10, truncate=False)

print("\n===== TIPO DE VEHÌCULO Y GRAVEDAD =====")
df.groupBy("tipo_vehiculo", "gravedad_accidente") \
    .count() \
    .orderBy(F.desc("count")) \
    .show(20, truncate=False)

spark.stop()

