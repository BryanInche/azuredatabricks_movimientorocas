import pandas as pd
import numpy as np
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import io


# a.Montar un servicio de datalake (podrias realizarlo primero en un jupyter notebook por separado)

#a.1 Get client_id, tenant_id and client_secret from key vault
# client_id = dbutils.secrets.get(scope = 'mlops-scope', key = 'mlops-app-client-id')
# tenant_id = dbutils.secrets.get(scope = 'mlops-scope', key = 'mlops-app-tenant-id')
# client_secret = dbutils.secrets.get(scope = 'mlops-scope', key = 'mlops-app-client-secret')

#a.2 #Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# configs = {"fs.azure.account.auth.type": "OAuth",
#           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
#           "fs.azure.account.oauth2.client.id": client_id,
#           "fs.azure.account.oauth2.client.secret": client_secret,
#           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

#a.3 Call file system utlity mount to mount the storage
# dbutils.fs.mount(
#  source = "abfss://presentation@datalakemlopsd4m.dfs.core.windows.net/",
#  mount_point = "/mnt/datalakemlopsd4m/presentation/",
#  extra_configs = configs)

#a.4 Verificar que contenedores estan montados en el azure datalake
#%fs mounts

#Cargar los datos a analizar
# 1. Obtener conection Azure DataLake,interfaz de Azure(Claves de acceso: Key1) (Debes comentar esta variable Si NO deja hacer COMMIT DEL 
# CODIGO EN GIT,GITHUB)
# connection_string = 'DefaultEndpointsProtocol=https;AccountName=datalakemlopsd4m;AccountKey=iWT8t74/#XlqcqoR03keDVtFZPzr0PB9zDffMPaLWMUBIAjUww8uYAVkc9xRkcBtvTmUHKBvd1sB3+ASt6mGgcQ==;EndpointSuffix=core.windows.net'

# 2. Conectar al Blob Storage de Azure
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# 2.1 Identificamos el nombre del contenedor(container_name) y nombre del archivo(definir blob_name) en el Blob Storage
container_name = "raw/proyectopases_raw/fuentedatos_c4m/operacion_marcobre/"

blob_name = "data_pases.parquet"

# 2.2 Obtener el blob_client
blob_client = blob_service_client.get_blob_client(container=container_name , blob=blob_name )

# 2.3 Leer el contenido del blob como un objeto de bytes
blob_data = blob_client.download_blob().readall()

# 2.4 Leer el archivo PARQUET en un DataFrame de Pandas desde el texto
datos = pd.read_parquet(io.BytesIO(blob_data))

datos.head()