#Librerias necesarias para desarrollar el modelo ml con Mlops
import pandas as pd
import mlflow
import mlflow.keras
import mlflow.tensorflow
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, LSTM
import numpy as np
import tensorflow as tf
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import matplotlib.pyplot as plt
import os

# 1. Cargar los datos para desarrollar y entrenar el modelo de machine learning
df_delta = spark.read.format("delta").load("/mnt/datalakemlopsd4m/presentation/proyectopases_presentation/datos_presentation_tabladelta_2024_04_08")
datos = df_delta.toPandas()

# 2. Opcional(Establecer la URI de seguimiento de MLflow si es necesario)
#mlflow.set_tracking_uri("databricks")  # Por ejemplo, para Databricks


# 3. Separar las variables independientes y dependiente
X = datos[['capacidad_en_volumen_equipo_carguio_m3',
           'capacidad_en_peso_equipo_carguio',
           'capacidad_en_peso_equipo_acarreo',
           'densidad_inicial_poligono_creado_tn/m3',
           'capacidad_en_volumen_equipo_acarreo_m3']].values
y = datos['numero_pases_carguio'].values

# 4. Construir el modelo de red neuronal
np.random.seed(42)
tf.random.set_seed(42)

X_train_rnn, X_test_rnn, y_train_rnn, y_test_rnn = train_test_split(X, y, test_size=0.2, random_state=42)

# 4. Establecer el nombre del experimento en la ruta deseada (en caso de script de python)
experiment_name = "/Users/bryan.inche@ms4m.com/proyectopases_experimentosMlFlow"   #Nombre del experimento que se creo en el workspace de Users
mlflow.set_experiment(experiment_name)                         #se crea el experimento
#mlflow.set_experiment("Default")

mlflow.tensorflow.autolog()  # Keras y TensorFlow

# 5.Entrenar el modelo
with mlflow.start_run(run_name='experimento_mlflow_1'):  #Ingresar el nombre de un nuevo experimento de MlFlow dentro del experimento set_experiment
    model_rnn = Sequential()
    model_rnn.add(LSTM(30, activation='relu', input_shape=(1, 5)))
    model_rnn.add(Dense(60, activation='relu'))
    model_rnn.add(Dense(30, activation='relu'))
    model_rnn.add(Dense(15, activation='relu'))
    model_rnn.add(Dense(1, activation='linear'))

    model_rnn.compile(loss='mean_squared_error', optimizer='adam')

    history = model_rnn.fit(X_train_rnn.reshape(-1, 1, 5), y_train_rnn, epochs=1, 
                            validation_data=(X_test_rnn.reshape(-1, 1, 5), y_test_rnn), 
                            batch_size=5, verbose=1)

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
    #mlflow.keras.log_model(model_rnn, "keras_tf_3_22_24")   #Modificar y debe coincidir con el nombre en model path

    # Calcular métricas
    train_loss = history.history['loss']
    test_loss = history.history['val_loss']
    train_rmse = np.sqrt(train_loss)
    test_rmse = np.sqrt(test_loss)

    # Registrar métricas de RMSE
    mlflow.log_metric("train_RMSE", train_rmse[-1])
    mlflow.log_metric("test_RMSE", test_rmse[-1])

    # Crear gráfico de pérdida durante el entrenamiento
    epochs = range(1, len(train_loss) + 1)
    plt.plot(epochs, train_loss, 'bo', label='Pérdida en entrenamiento')
    plt.plot(epochs, test_loss, 'r', label='Pérdida en validación')
    plt.title('Pérdida durante el entrenamiento y la validación')
    plt.xlabel('Épocas')
    plt.ylabel('Pérdida')
    plt.legend()
    plt.savefig("plot_3_27_24.png")
    mlflow.log_artifact("plot_3_27_24.png")
