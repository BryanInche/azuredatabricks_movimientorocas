
# Registrar un modelo en MLflow
model_path = "dbfs:/databricks/mlflow-tracking/8323b80e926241ee8763b7dc43085602/3fdcef4bd83943ec9dc2188bc13045f4/artifacts/model"
model_name = "MLops_kerastf_v1"
mlflow.register_model(model_path, model_name)