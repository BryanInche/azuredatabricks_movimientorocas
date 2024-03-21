#Librerias para redes neuronales(LSTM)
import pandas as pd
import sklearn.model_selection
import sklearn.ensemble

#Librerias de Mlflow
import mlflow
import mlflow.keras
import mlflow.tensorflow
from hyperopt import fmin, tpe, hp, SparkTrials, Trials, STATUS_OK
from hyperopt.pyll import scope

from tensorflow.keras.layers import Dense, LSTM
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Normalization
from tensorflow.keras.optimizers import Adam
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import matplotlib.pyplot as plt

# 1. Cargamos los datos para construir el Modelo Machine Learning
df_delta = spark.read.format("delta").load("/mnt/datalakemlopsd4m/presentation/datmarcobre_fengineer_tablacaract_delta")
datos = df_delta.toPandas()

# 2. Separamos las Variables Independientes y Variable Dependiente
# Supongamos que datos es tu DataFrame y has seleccionado tus características (X) y variable objetivo (y)
X = datos[['capacidad_en_volumen_equipo_carguio_m3',
'capacidad_en_peso_equipo_carguio',
'capacidad_en_peso_equipo_acarreo',
'densidad_inicial_poligono_creado_tn/m3',
'capacidad_en_volumen_equipo_acarreo_m3']].values
y = datos['numero_pases_carguio'].values # Reemplaza 'variable_objetivo' con el nombre de tu variable objetivo

# 3. Para que todos los experimentos de modelos machine learning se ejecuten Enable MLflow autologging for this notebook
#mlflow.autolog()  # Sklearn
mlflow.tensorflow.autolog()  # Keras y TensorFlow

# 4. Construccion del modelo de RED NEURONAL
# Aseguramos que los resultados sean "reproducibles" en cada ejecucion de tensorflow(pesos iniciales aleatorios)
np.random.seed(42)
tf.random.set_seed(42)

# Establecer semillas en NumPy y TensorFlow
np.random.seed(42)
tf.random.set_seed(42)

# Dividir los datos en conjuntos de entrenamiento y prueba
X_train_rnn, X_test_rnn, y_train_rnn, y_test_rnn = train_test_split(X, y, test_size=0.2, random_state=42)
#X_train_rnn, X_test_rnn, y_train_rnn, y_test_rnn = train_test_split(X_array, y_array, test_size=0.2, random_state=42)

# 4.2 Arquitectura 1 Red Neuronal:

mlflow.tensorflow.autolog()  # Keras y TensorFlow

# 4.3 Entrenar el modelo
with mlflow.start_run(run_name='experimento_mlflow_keras_tf_3-21-24'):  #Iniciliza un nuevo experimento de MlFlow
    # Definir y compilar el modelo
    model_rnn = Sequential()
    model_rnn.add(LSTM(30, activation='relu', input_shape=(1, 5)))
    model_rnn.add(Dense(60, activation='relu'))
    model_rnn.add(Dense(30, activation='relu'))
    model_rnn.add(Dense(15, activation='relu'))
    model_rnn.add(Dense(1, activation='linear'))

    model_rnn.compile(loss='mean_squared_error', optimizer='adam')

    # Entrenar el modelo y guardar el historial del entrenamiento
    history = model_rnn.fit(X_train_rnn.reshape(-1, 1, 5), y_train_rnn, epochs=1, validation_data=(X_test_rnn.reshape(-1, 1, 5), y_test_rnn), batch_size=5, verbose=1)

    # Save the run information to register the model later
    #kerasURI = run.info.artifact_uri

    # Registrar los parámetros del modelo
    mlflow.log_params({
        "input_shape": (1, 5),
        "lstm_units": 30,
        "dense_layers": [60, 30, 15],
        "activation": "relu",
        "optimizer": "adam",
        "loss_function": "mean_squared_error",
        "epochs": 1,
        "batch_size": 5
    })

    # Registrar la estructura del modelo en MLflow
    #mlflow.keras.log_model(model_rnn, "keras_tf_v1")

    # VALIDACION 
    # Obtener la pérdida en el conjunto de entrenamiento y el conjunto de validación
    train_loss = history.history['loss']
    test_loss = history.history['val_loss']

    # Calcular el RMSE en el conjunto de entrenamiento y el conjunto de validación
    train_rmse = np.sqrt(train_loss)
    test_rmse = np.sqrt(test_loss)

    # Registro de métricas de RMSE
    mlflow.log_metric("train_RMSE", train_rmse[-1])  # Se registra el último valor de RMSE del conjunto de entrenamiento
    mlflow.log_metric("test_RMSE", test_rmse[-1])      # Se registra el último valor de RMSE del conjunto de validación
    
    # Crear gráfico de pérdida durante el entrenamiento
    epochs = range(1, len(train_loss) + 1)
    plt.plot(epochs, train_loss, 'bo', label='Pérdida en entrenamiento')
    plt.plot(epochs, test_loss, 'r', label='Pérdida en validación')
    plt.title('Pérdida durante el entrenamiento y la validación')
    plt.xlabel('Épocas')
    plt.ylabel('Pérdida')
    plt.legend()
    plt.savefig("kerasplot-3-21-24.png")
    mlflow.log_artifact("kerasplot-3-21-24.png")

    # Registrar el modelo en MLflow y obtener el model_path
    mlflow.keras.log_model(model_rnn, "keras_tf_-3-21-24")
    
    # Obtener el model_path del modelo registrado
    model_path = mlflow.get_artifact_uri("keras_tf_-3-21-24")

# Registrar el modelo en MLflow utilizando el model_path obtenido
model_name = "MLops_kerastf_3-21-24"
mlflow.register_model(model_path, model_name)