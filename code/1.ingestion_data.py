import pandas as pd
from extraccion_datospostgres import consultar_postgres_y_obtener_df
from pyspark.sql.functions import when, col

# 1. Llamar a la función para ejecutar la consulta y obtener el DataFrame
datos = consultar_postgres_y_obtener_df()

# 2. Convertir el df-pandas  a un DataFrame de Spark
spark_datos = spark.createDataFrame(datos)

# 3. Listar todas las columnas y Convertir valores 'void' a cadena vacía ('') en todas las columnas que contienen 'void'
columnas = spark_datos.columns
for columna in columnas:
    spark_datos = spark_datos.withColumn(
        columna,
        when(col(columna) == 'void', '').otherwise(col(columna))
    )

# 4. Ruta donde quieres guardar el archivo CSV en tu Azure Data Lake Storage
ruta_guardado = "/mnt/datalakemlopsd4m/raw/marcobre/datosraw/datostotalmarcobre.csv" #cambiar el nombre del .csv al nombre deseado

# 5. Guardar el DataFrame en formato CSV en la ruta especificada
spark_datos.write.csv(ruta_guardado, header=True, mode="overwrite")
