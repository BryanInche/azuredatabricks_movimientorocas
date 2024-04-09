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


#4. #Get client_id, tenant_id and client_secret from key vault
#client_id = dbutils.secrets.get(scope = 'mlops-scope', key = 'mlops-app-client-id')
#tenant_id = dbutils.secrets.get(scope = 'mlops-scope', key = 'mlops-app-tenant-id')
#client_secret = dbutils.secrets.get(scope = 'mlops-scope', key = 'mlops-app-client-secret')

#4.1 #Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# configs = {"fs.azure.account.auth.type": "OAuth",
#           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
#           "fs.azure.account.oauth2.client.id": client_id,
#           "fs.azure.account.oauth2.client.secret": client_secret,
#           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

#4.2 #Montar el servicio de datalake, para el storage presentation en este ejemplo
# dbutils.fs.mount(
#  source = "abfss://presentation@datalakemlopsd4m.dfs.core.windows.net/",
#  mount_point = "/mnt/datalakemlopsd4m/raw/",
#  extra_configs = configs)

# 4.3 Ruta donde quieres guardar el archivo CSV en tu Azure Data Lake Storage
#ruta_guardado = "/mnt/datalakemlopsd4m/raw/marcobre/datosraw/datostotalmarcobre.csv" #cambiar el nombre del .csv al nombre deseado
ruta_guardado = "/mnt/datalakemlopsd4m/raw/proyectopases_raw/fuentedatos_c4m/operacion_marcobre/datos_raw_marcobre_2024_04_08.csv" #nombre del .csv

# 5. Guardar el DataFrame en formato CSV en la ruta especificada
spark_datos.write.csv(ruta_guardado, header=True, mode="overwrite")