#Librerias necesarias para desplegar el modelo machine learning en un ENDPOINT
from mlflow.tracking import MlflowClient
import requests
import json

# 1. Definir los parametros para la creacion del ENDPOINT (necesario cambiar cada vez que se ejecute esta tarea)
endpoint_name = "mlops_keras_endpoint_oflima"  #nombre que le daras al endpoint
model_name = "MLops_kerastf_oflima_v1"        #nombre del modelo alojado en MLFLOW Registry

#ELEGIR UNA VERSION EN ESPECIFICO
#specific_model_version = 1  # Especifica la versión deseada del modelo
#Accede a la versión específica del modelo
#model_version = MlflowClient().get_model_version(model_name, specific_model_version).version

#ELEGIR LA ULTIMA VERSION DISPONIBLE
model_version = MlflowClient().get_registered_model(model_name).latest_versions[0].version 
workload_type = "CPU"  #tipo de carga de trabajo que se ejecutará en el endpoint de producción. Puede ser CPU, GPU_SMALL, GPU_LARGE
workload_size = "Small"  # indica el tamaño del recurso de cómputo que se asignará al endpoint de producción. Puede ser Small, Medium, Large
scale_to_zero = True  # Si se establece en True, el sistema puede apagar automáticamente el endpoint cuando no esté recibiendo solicitudes


#2. Obtener la URL de la API que se utilizará para interactuar con el servicio de MLflow
API_ROOT = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get() 

#3. Obtener el Token de autorización necesario para autenticar las solicitudes a la API
API_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()  

#4. Creacion del endpoint
# 4.1 Cabeceras de la solicitud HTTP
data = {
    "name": endpoint_name,
    "config": {
        "served_entities": [
            {
                "entity_name": model_name,
                "entity_version": model_version,
                "workload_size": workload_size,
                "scale_to_zero_enabled": scale_to_zero,
                "workload_type": workload_type,
            }
        ]
    },
}

# 5. Token de autorización 
headers = {"Context-Type": "text/json", "Authorization": f"Bearer {API_TOKEN}"}

# 6. Se realiza una solicitud POST a la URL de la API de MLflow para crear el endpoint de producción
response = requests.post(
    url=f"{API_ROOT}/api/2.0/serving-endpoints", json=data, headers=headers
)

print(json.dumps(response.json(), indent=4))

# entrada en el navegador del enpoint para hacer la prediccion desde un jason
# {
#   "inputs": [[[12, 45, 129, 1, 129]]],
#   "params": {"max_new_tokens": 100, "temperature": 1}
# }