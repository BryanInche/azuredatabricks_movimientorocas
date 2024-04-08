import mlflow

# Nombre del modelo registrado en MlFlow Registry
model_name = "ModeloPases_rnn_v01"

# Crear una instancia del cliente de MLflow
client = mlflow.tracking.MlflowClient()

# Obtener la información sobre el modelo
model_info = client.get_registered_model(model_name)

# Estado al que deseas cambiar el modelo (p. ej., "Production", "Staging", "Archived")
new_stage = "Production"

# Versión específica que deseas cambiar de estado
target_version = model_info.latest_versions[0].version # ultima version del modelo registrado en mlflow registry
#target_version = 3     #Especificar una version del modelo, si es conveniente

# Cambiar el estado del modelo
client.transition_model_version_stage(
    name=model_name,
    version = target_version,  #
    stage=new_stage
)