import pandas as pd
from extraccion_datospostgres import consultar_postgres_y_obtener_df
from pyspark.sql.functions import when, col
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient #Librerias para obtener los accesos del Contanier de Azure Data Lake
from io import BytesIO

# 1. Llamar a la función para ejecutar la consulta y obtener el DataFrame
datos = consultar_postgres_y_obtener_df()

# # x. Convertir el df-pandas  a un DataFrame de Spark
# spark_datos = spark.createDataFrame(datos)

# # x. Listar todas las columnas y Convertir valores 'void' a cadena vacía ('') en todas las columnas que contienen 'void'
# columnas = spark_datos.columns
# for columna in columnas:
#     spark_datos = spark_datos.withColumn(
#         columna,
#         when(col(columna) == 'void', '').otherwise(col(columna))
#     )

# 2.Guardar el df en formato Parquet en la variable parquet_data
parquet_data = datos.to_parquet(engine='pyarrow')

#x. #Get client_id, tenant_id and client_secret from key vault
#client_id = dbutils.secrets.get(scope = 'mlops-scope', key = 'mlops-app-client-id')
#tenant_id = dbutils.secrets.get(scope = 'mlops-scope', key = 'mlops-app-tenant-id')
#client_secret = dbutils.secrets.get(scope = 'mlops-scope', key = 'mlops-app-client-secret')

#x.1 #Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# configs = {"fs.azure.account.auth.type": "OAuth",
#           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
#           "fs.azure.account.oauth2.client.id": client_id,
#           "fs.azure.account.oauth2.client.secret": client_secret,
#           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

#x.2 #Montar el servicio de datalake, para el storage presentation en este ejemplo
# dbutils.fs.mount(
#  source = "abfss://presentation@datalakemlopsd4m.dfs.core.windows.net/",
#  mount_point = "/mnt/datalakemlopsd4m/raw/",
#  extra_configs = configs)

# 3. Obtener conection Azure DataLake,interfaz de Azure(Claves de acceso: Key1) (Debes comentar esta variable Si NO deja hacer COMMIT DEL 
# CODIGO EN GIT,GITHUB)
connection_string = 'DefaultEndpointsProtocol=https;AccountName=datalakemlopsd4m;AccountKey=iWT8t74/#XlqcqoR03keDVtFZPzr0PB9zDffMPaLWMUBIAjUww8uYAVkc9xRkcBtvTmUHKBvd1sB3+ASt6mGgcQ==;EndpointSuffix=core.windows.net'

# 3.1 Conectar al Blob Storage de Azure
blob_service_client = BlobServiceClient.from_connection_string(connection_string)


# # x.3 Ruta donde quieres guardar el archivo CSV en tu Azure Data Lake Storage
# #ruta_guardado = "/mnt/datalakemlopsd4m/raw/marcobre/datosraw/datostotalmarcobre.csv" #cambiar el nombre del .csv al nombre deseado
# ruta_guardado = "/mnt/datalakemlopsd4m/raw/proyectopases_raw/fuentedatos_c4m/operacion_marcobre/datos_raw_marcobre_2024_04_08.csv" #nombre del .csv

# 4. Identificamos el nombre del contenedor(container_name) y nombre del archivo(definir blob_name) en el Blob Storage
container_name = "raw/proyectopases_raw/fuentedatos_c4m/operacion_hudbay/"

####### 4.1 !IMPORTANTE!: CAMBIAR EL NOMBRE DEL CSV (PARA EL EQUIPO EN PARTICULAR)  ##########################################################
blob_name = "datos_raw_hudbay.parquet"


# 5. Guardamos el archivo PARQUET al Blob Storage
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
# Si existe un archivo con el el mismo nombre, dara error (en ese caso deberias eliminar primero el archivo en el Azure Storage)

# 6. Verificar si existe el archivo
if blob_client.exists():
    # Eliminar el archivo existente
    blob_client.delete_blob()

# 7. Cargar el nuevo archivo Parquet al Blob Storage
blob_client.upload_blob(parquet_data)
