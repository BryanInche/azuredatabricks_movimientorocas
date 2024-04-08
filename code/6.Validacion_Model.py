import numpy as np
import mlflow
import mlflow.pyfunc
from sklearn.model_selection import train_test_split
import tensorflow as tf
from sklearn.metrics import mean_squared_error, mean_absolute_error

client = mlflow.tracking.MlflowClient() # Permite acceder a diversas funcionalidades de MLflow (incluyendo el acceso a modelos registrados en MLflow Registry)
model_name = "ModeloPases_rnn_v01"      #Ingresar el Nombre del Modelo Registrado en MlFlow Registry

model_info = client.get_registered_model(model_name) # Utiliza el cliente de MLflow para obtener información sobre el modelo registrado.
model_version = model_info.latest_versions[0].version # Ultima version del modelo en MlFlow Registry
#model_version = 1                                    #ingresar una version en especifico si asi lo consideras

# Cargar el modelo desde MLflow Registry
try:
    model = mlflow.pyfunc.load_model(model_uri=f"models:/{model_name}/{model_version}")
    print("Modelo cargado correctamente desde MLflow Registry.")
except Exception as e:
    print("Error al cargar el modelo desde MLflow Registry:", e)

# Preparar tus datos de validación (X_val)
df_delta = spark.read.format("delta").load("/mnt/datalakemlopsd4m/processed/datmarcobre_featureengineer_delta")
datos = df_delta.toPandas()
# Supongamos que datos es tu DataFrame y has seleccionado tus características (X) y variable objetivo (y)
X = datos[['capacidad_en_volumen_equipo_carguio_m3',
'capacidad_en_peso_equipo_carguio',
'capacidad_en_peso_equipo_acarreo',
'densidad_inicial_poligono_creado_tn/m3',
'capacidad_en_volumen_equipo_acarreo_m3']].values
y = datos['numero_pases_carguio'].values # Reemplaza 'variable_objetivo' con el nombre de tu variable objetivo

# Establecer semillas en NumPy y TensorFlow
np.random.seed(42)
tf.random.set_seed(42)

# Dividir los datos en conjuntos de entrenamiento y prueba
X_train_rnn, X_test_rnn, y_train_rnn, y_test_rnn = train_test_split(X, y, test_size=0.2, random_state=42)
X_test_rnn = X_test_rnn.reshape(-1, 1, 5)

# Realizar predicciones
try:
    predictions = model.predict(X_test_rnn)
    print("Predicciones realizadas exitosamente.")
except Exception as e:
    print("Error al realizar predicciones:", e)

# Calcular métricas
mse = mean_squared_error(y_test_rnn, predictions)
rmse = np.sqrt(mse)
mae = mean_absolute_error(y_test_rnn, predictions)

print("Métricas de error:")
print("MSE:", mse)
print("RMSE:", rmse)
print("MAE:", mae)

print("Modelo validado para migrar a ambiente de Producccion")