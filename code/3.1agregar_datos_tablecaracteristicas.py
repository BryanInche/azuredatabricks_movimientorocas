# 1. Cargar los datos originales
# 1. Cargar los datos para desarrollar y entrenar el modelo de machine learning
df_delta = spark.read.format("delta").load("/mnt/datalakemlopsd4m/presentation/proyectopases_presentation/datos_presentation_tabladelta_2024_04_08")
# 2. Convertir los datos a Pandas DataFrame
datos = df_delta.toPandas()

# 3. Agregar nuevos datos al DataFrame 'datos'
# Supongamos que tienes un nuevo DataFrame llamado 'nuevos_datos' que contiene los nuevos datos
df_delta_add = spark.read.format("delta").load("/mnt/datalakemlopsd4m/presentation/proyectopases_presentation/nuevosdatos")
# 3. Convertir los datos a Pandas DataFrame
datos_add = df_delta_add.toPandas()

# Aquí se concatena el DataFrame 'nuevos_datos' con el DataFrame original 'datos'
datos_actualizados = pd.concat([datos, nuevos_datos], ignore_index=True)

# 4. Convertir los datos actualizados a DataFrame Spark
df_actualizado = spark.createDataFrame(datos_actualizados)

# 5. Guardar la tabla Delta actualizada
df_actualizado.write.format("delta").mode("overwrite").save("/mnt/datalakemlopsd4m/presentation/proyectopases_presentation/datos_actualizados")
