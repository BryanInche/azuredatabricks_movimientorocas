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

# 1. Establecer la URI de seguimiento de MLflow si es necesario
mlflow.set_tracking_uri("databricks")  # Por ejemplo, para Databricks

# 2. Establecer el nombre del experimento en la ruta deseada
experiment_name = "/Repos/bryan.inche@ms4m.com/pases-mlops-project/artifacts/experimento2"
#experiment_name = "/Users/bryan.inche@ms4m.com/experimento1"
mlflow.set_experiment(experiment_name)

# 3. Cargar los datos para desarrollar y entrenar el modelo de machine learning
df_delta = spark.read.format("delta").load("/mnt/datalakemlopsd4m/presentation/datmarcobre_fengineer_tablacaract_delta")
datos = df_delta.toPandas()

# 4. Separar las variables independientes y dependiente
X = datos[['capacidad_en_volumen_equipo_carguio_m3',
           'capacidad_en_peso_equipo_carguio',
           'capacidad_en_peso_equipo_acarreo',
           'densidad_inicial_poligono_creado_tn/m3',
           'capacidad_en_volumen_equipo_acarreo_m3']].values
y = datos['numero_pases_carguio'].values

# 5. Construir el modelo de red neuronal
np.random.seed(42)
tf.random.set_seed(42)

X_train_rnn, X_test_rnn, y_train_rnn, y_test_rnn = train_test_split(X, y, test_size=0.2, random_state=42)

mlflow.tensorflow.autolog()  # Keras y TensorFlow

# 6.Entrenar el modelo
with mlflow.start_run(run_name='experimento_mlflow_keras_tf_oflimav1'):  #Iniciliza un nuevo experimento de MlFlow
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
    mlflow.keras.log_model(model_rnn, "keras_tf_3_22_24")

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
    plt.savefig("kerasplot_3_22_24.png")
    mlflow.log_artifact("kerasplot_3_22_24.png")

    # Obtener la ruta del modelo registrado
    model_path = mlflow.get_artifact_uri("keras_tf_3_22_24")

# Registrar el modelo en MLflow
model_name = "MLops_kerastf_oflima_v1"
mlflow.register_model(model_path, model_name)
