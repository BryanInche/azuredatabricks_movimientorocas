#Librerias para redes neuronales(LSTM)
from tensorflow.keras.layers import Dense, LSTM
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Normalization
from tensorflow.keras.optimizers import Adam
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import tensorflow as tf
import numpy as np
import tensorflow as tf
import numpy as np

# 1. Cargamos los datos para construir el Modelo Machine Learning
df_delta = spark.read.format("delta").load("/mnt/datalakemlopsd4m/processed/datmarcobre_featureengineer_delta")
datos = df_delta.toPandas()

# 2. Separamos las Variables Independientes y Variable Dependiente
# Supongamos que datos es tu DataFrame y has seleccionado tus características (X) y variable objetivo (y)
X = datos[['capacidad_en_volumen_equipo_carguio_m3',
'capacidad_en_peso_equipo_carguio',
'capacidad_en_peso_equipo_acarreo',
'densidad_inicial_poligono_creado_tn/m3',
'capacidad_en_volumen_equipo_acarreo_m3']].values
y = datos['numero_pases_carguio'].values # Reemplaza 'variable_objetivo' con el nombre de tu variable objetivo

# 3. Construccion del modelo de RED NEURONAL(LSTM)
# Aseguramos que los resultados sean "reproducibles" en cada ejecucion de tensorflow(pesos iniciales aleatorios)
np.random.seed(42)
tf.random.set_seed(42)

# Dividir los datos en conjuntos de entrenamiento y prueba
X_train_rnn, X_test_rnn, y_train_rnn, y_test_rnn = train_test_split(X, y, test_size=0.2, random_state=42)
#X_train_rnn, X_test_rnn, y_train_rnn, y_test_rnn = train_test_split(X_array, y_array, test_size=0.2, random_state=42)

model_rnn = Sequential()
# La forma de entrada para LSTM debe ser (n_timesteps, n_features)
model_rnn.add(LSTM(30, activation='relu', input_shape=(1, 5)))  #  (n_samples, n_pasos, n_variables)
# Agregar capas Dense según sea necesario
model_rnn.add(Dense(60, activation='relu'))
model_rnn.add(Dense(30, activation='relu'))
model_rnn.add(Dense(15, activation='relu'))
# Capa de salida
model_rnn.add(Dense(1, activation='linear'))

# Compilar el modelo
model_rnn.compile(loss='mean_squared_error', optimizer='adam')
# Imprimir un resumen del modelo
model_rnn.summary()

# Entrenar el modelo
history = model_rnn.fit(X_train_rnn.reshape(-1, 1, 5), y_train_rnn, epochs=5, validation_data=(X_test_rnn.reshape(-1, 1, 5),y_test_rnn), batch_size=5, verbose=1)

# 4. Validacion del Modelo con la metrica de RSME
# VALIDACION 1 del Modelo
# Obtener la pérdida en el conjunto de entrenamiento y el conjunto de validación
train_loss = history.history['loss']
val_loss = history.history['val_loss']

# Calcular el RMSE en el conjunto de entrenamiento y el conjunto de validación
train_rmse = np.sqrt(train_loss)
val_rmse = np.sqrt(val_loss)

# VALIDACION 2 del Modelo
# Hacer predicciones en el conjunto de prueba
y_pred_rnn = model_rnn.predict(X_test_rnn.reshape(-1, 1, 5)) # (n_samples, n_pasos, n_variables)

# Redondear los valores de y_pred al entero más cercano
y_pred_rnn = np.round(y_pred_rnn).astype('int64')

# Calcular el error cuadrático medio en el conjunto de prueba
mse = mean_squared_error(y_test_rnn, y_pred_rnn)
# Calcular la raíz cuadrada del error cuadrático medio
rmse = np.sqrt(mse)
print(f'Raíz del Error Cuadrático Medio en el conjunto de prueba: {rmse}')

# 5. Guardar el Modelo ML 
# Guardando todos los componentes del modelo (arquitectura, pesos, configuración de entrenamiento, estado del optimizador, etc.)
# SavedModel de TensorFlow
model_rnn.save("/modelos/model_rnn_pasesv2")


