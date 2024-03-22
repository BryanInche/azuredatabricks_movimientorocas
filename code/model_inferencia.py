import numpy as np
import requests
import json

# 1. Datos de entrada
input_data = np.array([[29, 35, 129, 4, 129]])

# 2. Remodelar los datos de entrada para que tengan la forma (None, 1, 5)
input_data_reshaped = np.reshape(input_data, (input_data.shape[0], 1, input_data.shape[1]))

# 3. Convertir los datos de entrada a una lista de Python para serializarlos a JSON
input_data_list = input_data_reshaped.tolist()

# 4. Obtener apiUrl de la API que se utilizará para interactuar con el servicio de MLflow
API_ROOT = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get() 

# 5. Obtener apiToken de autorización necesario para autenticar las solicitudes a la API
API_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()  

# 6. Ingresar el nombre del ENDPOINT A UTILIZAR (Modelo ML alojado en dicho endpoint)
endpoint_name = "mlops_kerasv1_endpoint"

data = {
  "inputs" : input_data_list,
  "params" : {"max_new_tokens": 100, "temperature": 1}
}

headers = {"Context-Type": "text/json", "Authorization": f"Bearer {API_TOKEN}"}

response = requests.post(
    url=f"{API_ROOT}/serving-endpoints/{endpoint_name}/invocations", json=data, headers=headers
)

print(json.dumps(response.json()))

# FORMATO de entrada en el navegador del enpoint para hacer la prediccion desde un jason
# {
#   "inputs": [[[12, 45, 129, 1, 129]]],
#   "params": {"max_new_tokens": 100, "temperature": 1}
# }