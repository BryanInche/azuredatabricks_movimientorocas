from fastapi import FastAPI, Form, HTTPException
from fastapi.responses import HTMLResponse
import requests

app = FastAPI()

# Endpoint del modelo en MLflow
MLFLOW_ENDPOINT = "https://adb-6335500359056740.0.azuredatabricks.net/serving-endpoints/mlops_keras_3_22_24_endpoint/invocations"

@app.post("/predict/")
async def predict(var1: float = Form(...), var2: float = Form(...), var3: float = Form(...), var4: float = Form(...), var5: float = Form(...)):
    """
    Realiza una predicción utilizando el modelo alojado en MLflow.
    
    Parámetros:
    - var1, var2, var3, var4, var5: Valores de las variables para la predicción.
    
    Retorna:
    - JSON con la predicción.
    """
    # Enviar los datos al endpoint de MLflow para inferir
    input_data = {"inputs": [[var1, var2, var3, var4, var5]]}
    response = requests.post(MLFLOW_ENDPOINT, json=input_data)
    
    # Verificar el código de estado de la respuesta
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail="Error al realizar la predicción")

    # Obtener la predicción del modelo
    prediction = response.json().get('prediction')
    if prediction is None:
        raise HTTPException(status_code=500, detail="No se pudo obtener la predicción del modelo")

    return {"prediction": prediction}

@app.get("/", response_class=HTMLResponse)
async def index():
    """
    Página de inicio con el formulario para enviar las variables de entrada.
    """
    return """
    <html>
        <head>
            <title>Predicción con modelo MLflow</title>
        </head>
        <body>
            <h1>Ingrese los valores de las variables para obtener una predicción</h1>
            <form method="post">
                <label for="var1">Variable 1:</label>
                <input type="number" name="var1"><br>
                <label for="var2">Variable 2:</label>
                <input type="number" name="var2"><br>
                <label for="var3">Variable 3:</label>
                <input type="number" name="var3"><br>
                <label for="var4">Variable 4:</label>
                <input type="number" name="var4"><br>
                <label for="var5">Variable 5:</label>
                <input type="number" name="var5"><br>
                <button type="submit">Predecir</button>
            </form>
        </body>
    </html>
    """
