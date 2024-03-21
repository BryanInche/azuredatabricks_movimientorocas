from flask import Flask, render_template, request, jsonify
import requests
import numpy as np
import json

app = Flask(__name__)

# Definir la URL y el nombre del endpoint de MLflow
API_ROOT = "https://adb-6335500359056740.0.azuredatabricks.net"
endpoint_name = "mlops_kerasv1_endpoint"

# Token de autorización necesario para autenticar las solicitudes a la API
API_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get() # Asegúrate de reemplazarlo con tu token de autenticación real

#GET para solicitar recursos del servidor,   POST se utilizan para enviar datos al servidor y hacer la prediccion
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # Obtener datos de entrada del formulario web
        input_data = np.array([[int(request.form['input1']), int(request.form['input2']), int(request.form['input3']), int(request.form['input4']), int(request.form['input5'])]])
        
        # Remodelar los datos de entrada para que tengan la forma (None, 1, 5)
        input_data_reshaped = np.reshape(input_data, (input_data.shape[0], 1, input_data.shape[1]))
        
        # Convertir los datos de entrada a una lista de Python para serializarlos a JSON
        input_data_list = input_data_reshaped.tolist()

        data = {
          "inputs" : input_data_list,
          "params" : {"max_new_tokens": 100, "temperature": 1}
        }

        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {API_TOKEN}"}

        # Enviar la solicitud POST al endpoint de servicio
        response = requests.post(
            url=f"{API_ROOT}/serving-endpoints/{endpoint_name}/invocations", json=data, headers=headers
        )

        # Mostrar los resultados devueltos por el modelo en la interfaz web
        return render_template('result.html', result=response.json())
    else:
        return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)
