import pandas as pd
#1. Leemos los datos de PROCEESED la tabla Delta 
df_delta = spark.read.format("delta").load("/mnt/datalakemlopsd4m/processed/datmarcobre_prepro2_delta")
datos = df_delta.toPandas()

# 2. Seleccionamos las variables que son mas relevantes para la construccion del modelo de ML
#nuevos_nombres = {'radiohexagonocuchara_equipo_carguio' : 'radiohexagonocuchara_equipocarguio',
#    'capacidad_vol_equipo_carguio':'capacidad_en_volumen_equipo_carguio_m3', 'capacidad_pes_equipo_carguio':'capacidad_en_peso_equipo_carguio'}

# 2.1 Renombra las columnas del DataFrame
#datos = datos.rename(columns=nuevos_nombres)
# 2.2 Filtro de variables relevantes
datos = datos[['tonelaje_inicial_poligono_creado','radiohexagonocuchara_equipocarguio','capacidad_en_volumen_equipo_carguio_m3','capacidad_en_peso_equipo_carguio',
'capacidad_en_peso_equipo_acarreo','tiempo_estimado_duracion_estado_pala','radio_locacion_metros','tiempo_ready_llegada_esperando','tiempo_ready_esperando_cuadra',
'tonelaje_camion_antes_cargaestabilizada','angulo_giro_promedio_pases', 'tonelaje_segun_computadora','id_equipo_camion', 'id_equipo_carguio',
'densidad_inicial_poligono_creado_tn/m3','numero_pases_carguio', 'productividad_operativa_acarreo_tn_h','tiempo_llegada_camion',
'tiempo_ready_cuadra_cuadrado','tiempo_ready_cuadrado_cargado','tiempo_ready_carga_acarreo','tiempo_ready_acarreo_cola', 'tiempo_ready_cola_retro',
'tiempo_ready_retro_listo','tiempo_ready_listo_descarga','tiempo_ready_descarga_viajandovacio','tiempo_ready_cargando_pala','tiempo_ready_esperando_pala',
'productividad_operativa_carguio_tn_h','al_termino_cargar_en_espera_cuadrado_cuadrandose','factor','capacidad_en_volumen_equipo_acarreo_m3']]

# 3. Tratamiiento de variables en especifico
#- Redondeamos el valor del tonelaje inicial, a un solo decimal
datos['tonelaje_inicial_poligono_creado'] = datos['tonelaje_inicial_poligono_creado'].round(1)

#- Convertimos de float a int64 la variable id_equipo_carguio
datos['id_equipo_carguio'] = datos['id_equipo_carguio'].round(0).astype('int64')

# n Antes de Guardar convertir el df-pandas Preprocesado a un DataFrame de Spark
spark_datos = spark.createDataFrame(datos)

# n.1 Guardamos los datos preprocesados en el Storage de Processed , en una Tabla DELTA
spark_datos.write.format("delta").mode("overwrite").saveAsTable("processed_db.datmarcobre_featureengineer_delta") #processed_db:Nombre de BD y datos_processed_delta:Nombre
