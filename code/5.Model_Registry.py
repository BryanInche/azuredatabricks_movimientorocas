import mlflow 

#1. Identificar el Id del Experimento que registraremos como Modelo en MlFlow
model_name = "ModeloPases_rnn_v01"                         #Ingrese nombre modelo que registraremos en MlFlow Registry(si es igual se hara version 2,3,...)
id_ejecucion = "e4d090da7d4d4f93b4b78c6ccacbe467"          #Ingrese id_ejecucion: identificador unico del experimento a registrar en Mlflow
model_uri = f"runs:/{id_ejecucion}/model"                  #url 
registered_model_version = mlflow.register_model(model_uri, model_name) #se registra el modelo(experimento) en Mlflow Registry

# Si deseas, puedes obtener el ID de la nueva versión del modelo registrado
version_id = registered_model_version.version
print("Ultima Version del Modelo regsitrado:",version_id)

# Crear una instancia del cliente de MLflow
client = mlflow.tracking.MlflowClient()

# Obtener la información sobre el modelo
model_info = client.get_registered_model(model_name)

# Estado al que deseas cambiar el modelo (p. ej., "Production", "Staging", "Archived")
new_stage = "Staging"

# Versión específica que deseas cambiar de estado
target_version = model_info.latest_versions[0].version # ultima version del modelo registrado en mlflow registry
#target_version = 1     #Especificar una version del modelo, si es conveniente

# Cambiar el estado del modelo
client.transition_model_version_stage(
    name=model_name,
    version = target_version,  #
    stage=new_stage
)