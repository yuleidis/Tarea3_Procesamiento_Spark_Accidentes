from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import os

spark = SparkSession.builder.appName("VisualizacionBatchAccidentes").getOrCreate()

ruta = "hdfs://localhost:9000/user/vboxuser/vehiculos_accidentes/vehiculos_accidentes.csv"

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(ruta)

df = df.dropDuplicates()
df = df.dropna(
    subset=[
        "marca_vehiculo",
        "tipo_vehiculo",
        "edad_vehiculo",
        "gravedad_accidente"
    ]
)

df = df.withColumn(
    "edad_vehiculo",
    F.col("edad_vehiculo").cast("integer")
)

df = df.filter(F.col("edad_vehiculo") >= 0)

carpeta = "/home/vboxuser/graficas_batch"
os.makedirs(carpeta, exist_ok=True)

# Grafica 1: cantidad de vehiculos por tipo
tipos = df.groupBy("tipo_vehiculo") \
    .count() \
    .orderBy(F.desc("count")) \
    .limit(10) \
    .collect()

nombres_tipos = [str(fila["tipo_vehiculo"]) for fila in tipos]
cantidades_tipos = [fila["count"] for fila in tipos]

plt.figure(figsize=(10, 6))
plt.bar(nombres_tipos, cantidades_tipos)
plt.title("Cantidad de vehiculos por tipo")
plt.xlabel("Tipo de vehiculo")
plt.ylabel("Cantidad de registros")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig(carpeta + "/vehiculos_por_tipo.png")
plt.close()

# Grafica 2: cantidad de registros por gravedad del accidente
gravedades = df.groupBy("gravedad_accidente") \
    .count() \
    .orderBy(F.desc("count")) \
    .collect()

nombres_gravedad = [str(fila["gravedad_accidente"]) for fila in gravedades]
cantidades_gravedad = [fila["count"] for fila in gravedades]

plt.figure(figsize=(9, 6))
plt.bar(nombres_gravedad, cantidades_gravedad)
plt.title("Cantidad de registros por gravedad del accidente")
plt.xlabel("Gravedad del accidente")
plt.ylabel("Cantidad de registros")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig(carpeta + "/accidentes_por_gravedad.png")
plt.close()

print("Graficas generadas correctamente.")
print("Carpeta de resultados:", carpeta)
print("Archivos:")
print("- vehiculos_por_tipo.png")
print("- accidentes_por_gravedad.png")

spark.stop()
