# 1. Cargamos los datos para construir el Modelo Machine Learning
df_delta = spark.read.format("delta").load("/mnt/datalakemlopsd4m/processed/datmarcobre_featureengineer_delta")
datos = df_delta.toPandas()

# 2. 