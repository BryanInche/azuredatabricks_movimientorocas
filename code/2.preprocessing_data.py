import pandas as pd
import numpy as np
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import io

# a.Montar un servicio de datalake (podrias realizarlo primero en un jupyter notebook por separado)

# a.1 Get client_id, tenant_id and client_secret from key vault
# client_id = dbutils.secrets.get(scope = 'mlops-scope', key = 'mlops-app-client-id')
# tenant_id = dbutils.secrets.get(scope = 'mlops-scope', key = 'mlops-app-tenant-id')
# client_secret = dbutils.secrets.get(scope = 'mlops-scope', key = 'mlops-app-client-secret')

# a.2 #Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# configs = {"fs.azure.account.auth.type": "OAuth",
#           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
#           "fs.azure.account.oauth2.client.id": client_id,
#           "fs.azure.account.oauth2.client.secret": client_secret,
#           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# a.3 Call file system utlity mount to mount the storage
# dbutils.fs.mount(
#  source = "abfss://presentation@datalakemlopsd4m.dfs.core.windows.net/",
#  mount_point = "/mnt/datalakemlopsd4m/presentation/",
#  extra_configs = configs)

# a.4 Verificar que contenedores estan montados en el azure datalake
# %fs mounts

# Cargar los datos a analizar
# 1. Obtener conection Azure DataLake,interfaz de Azure(Claves de acceso: Key1) (Debes comentar esta variable Si NO deja hacer COMMIT DEL 
# CODIGO EN GIT,GITHUB)
# connection_string = 'DefaultEndpointsProtocol=https;AccountName=datalakemlopsd4m;AccountKey=iWT8t74/#XlqcqoR03keDVtFZPzr0PB9zDffMPaLWMUBIAjUww8uYAVkc9xRkcBtvTmUHKBvd1sB3+ASt6mGgcQ==;EndpointSuffix=core.windows.net'

# # 2. Conectar al Blob Storage de Azure
# blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# # 2.1 Identificamos el nombre del contenedor(container_name) y nombre del archivo(definir blob_name) en el Blob Storage
# container_name = "raw/proyectopases_raw/fuentedatos_c4m/operacion_marcobre/"

# #blob_name = "data_pases.csv"
# blob_name = "data_pases.parquet"

# # 2.2 Obtener el blob_client
# blob_client = blob_service_client.get_blob_client(container=container_name , blob=blob_name)

# # 2.3 Leer el contenido del blob como texto (en caso sea CSV),  Texto delimitado por comas
# #blob_data = blob_client.download_blob().content_as_text()
# # 2.3 Leer el contenido del blob como un objeto de bytes (en caso sea Parquet), Binario, almacenamiento en columnas
# blob_data = blob_client.download_blob().readall()

# #2.4 Leer el archivo CSV en un DataFrame de Pandas
# #datos = pd.read_csv(io.StringIO(blob_data))
# # 2.4 Leer el archivo PARQUET en un DataFrame de Pandas
# datos = pd.read_parquet(io.BytesIO(blob_data))

# datos.head()

#1. Leemos Datos de Zona Row
ruta_carpeta_csv = "/mnt/datalakemlopsd4m/raw/proyectopases_raw/fuentedatos_c4m/operacion_marcobre/datos_raw_marcobre_2024_04_08.csv"
df_spark = spark.read.option("header", "true").csv(ruta_carpeta_csv)
 
# 2. Muestra los primeros registros del DataFrame de Spark Convertido a un DataFrame Pandas
datos = df_spark.toPandas()

# 3.Tratamiento de valores Nulos
# 3.1 Supongamos que tienes un DataFrame llamado datos
valores_nulos = datos.isnull().sum()
valores_nulos_ordenados = valores_nulos.sort_values(ascending=False)
porcentaje_nulos = (valores_nulos_ordenados / len(datos)) * 100

# 3.2 Eliminamos las variables que tienen mas de 80% Nulos
columnas_a_eliminar = porcentaje_nulos[porcentaje_nulos > 80].index
datos = datos.drop(columnas_a_eliminar, axis=1)

#4. Tratamiento de variables 
#4.1 Eliminando columnas especificas que no aportan informacion
datos = datos.drop(['tipoubicacionsupervisor_camion','tipoubicacionsupervisor_pala', 'id_cargadescarga_pases','dumpreal','loadreal', 'rownum_global','turno'], axis=1)

# 3.2 Calcula la moda de 'has_block_pases' y Completa los valores nulos con la moda en la columna 'has_block_pases'
moda_has_block_pases = datos['has_block_pases'].mode()[0]
datos['has_block_pases'].fillna(moda_has_block_pases, inplace=True)

# 3.3 Calcula la moda de 'tipodescargaidentifier' y completa los valores nulos con la moda en la columna 'tipodescargaidentifier'
moda_tipodescargaidentifier = datos['tipodescargaidentifier'].mode()[0]
datos['tipodescargaidentifier'].fillna(moda_tipodescargaidentifier, inplace=True)

# 3.4 Rellenar los valores nulos con ceros en todo el DataFrame
datos = datos.fillna(0)


#4. Transformacion de Datos Tiempo a formato Datetime
# 4.1 Supongamos que 'datos' es tu DataFrame y 'columnas_fecha' son las columnas que contienen fechas
columnas_fecha = ['tiem_llegada_global', 'tiem_esperando', 'tiem_cuadra', 'tiem_cuadrado', 'tiem_carga', 'tiem_acarreo', 'tiem_cola', 'tiem_retro', 'tiem_listo', 'tiem_descarga', 'tiem_viajando', 'tiempo_inicio_carga_carguio', 'tiempo_esperando_carguio', 'previous_esperando_pala', 'tiempo_inicio_cambio_estado_camion', 'tiempo_inicio_cambio_estado_pala']
# 4.2 Convertir todas las fechas al mismo formato
for columna in columnas_fecha:
    datos[columna] = pd.to_datetime(datos[columna], errors='coerce') #Si hubiese una fecha con  Error, lo reemplaza con NAT
# 4.3 Reemplazar los valores nulos con la fecha de la fila anterior más 3 segundos adicionales
for columna in columnas_fecha:
    mask_nat = datos[columna].isna()
    datos[columna].loc[mask_nat] = datos[columna].fillna(method='ffill') + pd.to_timedelta(3, unit='s')
# 4.4 Formatear las fechas en el formato deseado
formato_deseado = "%Y-%m-%d %H:%M:%S.%f%z"  # Formato deseado
for columna in columnas_fecha:
    datos[columna] = datos[columna].dt.strftime(formato_deseado)
    datos[columna] = pd.to_datetime(datos[columna])
    datos[columna] = datos[columna] + pd.to_timedelta(999, unit='ms')
    #datos[columna] = pd.to_datetime(datos[columna])

#5. Eliminamos los filas duplicadas
datos = datos.drop_duplicates()


# 6. Transformacion a Tipo de datos adecuado formato para las variables
# 6.1 Diccionario para especificar los tipos de datos deseados para cada columna
tipos_de_datos = {
    'id_ciclo_acarreo': 'int64','id_cargadescarga': 'int64','id_palas': 'int64','id_equipo_camion': 'int64','id_ciclo_carguio': 'float64',
    'id_equipo_carguio': 'float64','id_trabajador_pala': 'float64','id_guardia_realiza_carga_al_camion': 'float64','id_locacion': 'float64',
    'id_poligono_se_obtiene_material': 'float64','tiempo_ready_cargando_pala': 'float64','tiempo_ready_esperando_pala': 'float64',
    'cantidad_equipos_espera_al_termino_carga_pala': 'float64','id_estados_camion': 'int64','id_equipo_table_estados_camion': 'int64',
    'id_detal_estado_camion': 'int64','tiempo_estimado_duracion_estado_camion': 'int64','en_campo_o_taller_mantenimiento_camion': 'int64',
    'id_tipo_estad_camion': 'int64','id_estados_pala': 'float64','id_equipo_table_estados_pala': 'float64','id_detal_estado_pala': 'float64',
    'tiempo_estimado_duracion_estado_pala': 'float64','en_campo_o_taller_mantenimiento_pala': 'float64','id_tipo_estad_pala': 'float64',
    'id_descarga': 'int64','id_factor': 'int64','id_poligono': 'float64','tiempo_ready_llegada_esperando': 'float64',
    'tiempo_ready_esperando_cuadra': 'float64','tiempo_ready_cuadra_cuadrado': 'float64', 'tiempo_ready_cuadrado_cargado': 'float64',
    'tiempo_ready_carga_acarreo': 'float64','tiempo_ready_acarreo_cola': 'float64','tiempo_ready_cola_retro': 'float64',
    'tiempo_ready_retro_listo': 'float64','tiempo_ready_listo_descarga': 'float64','tiempo_ready_descarga_viajandovacio': 'float64',
    'id_trabajador_camion': 'int64','id_palanext': 'int64','tonelajevims': 'float64','yn_estado': 'bool',
    'id_guardia_hizocarga': 'int64','id_guardia_hizodescarga': 'int64','id_zona_aplicafactor': 'int64','id_zona_pertenece_poligono': 'float64',
    'factor': 'int64','toneladas_secas': 'float64','productividad_operativa_acarreo_tn_h': 'float64','productividad_operativa_carguio_tn_h': 'float64','efhcargado': 'float64','efhvacio': 'float64','distrealcargado': 'float64','distrealvacio': 'float64','coorxdesc': 'float64',
    'coorydesc': 'float64','coorzdesc': 'float64','tipodescargaidentifier': 'float64','tonelajevvanterior': 'int64','tonelajevvposterior': 'float64','velocidadvimscargado': 'float64','velocidadvimsvacio': 'float64','velocidadgpscargado': 'float64','velocidadgpsvacio': 'float64','tonelajevimsretain': 'float64', 'nivelcombuscargado': 'float64','nivelcombusdescargado':'float64',
    'volumen': 'float64','aplicafactor_vol': 'bool','coorzniveldescarga': 'float64','efh_factor_loaded': 'float64','efh_factor_empty':'float64',
    'id_secundario': 'int64','id_principal': 'int64','capacidad_vol_equipo': 'float64','capacidad_pes_equipo': 'float64',
    'capacidadtanque_equipo': 'int64','peso_bruto_equipo': 'float64','ishp_equipo': 'bool','ancho_equipo': 'int64','largo_equipo': 'int64',
    'numeroejes_equipo': 'int64','id_turnos_turnocarga': 'int64','horaini_turnocarga': 'int64','horafin_turnocarga': 'int64',
    'id_turnos_turnodescarga': 'int64','horaini_turnodescarga': 'int64','horafin_turnodescarga': 'int64',
    'id_zona_encuentra_descarga': 'int64','id_nodo_carga': 'float64','id_nodo_descarga': 'int64',
    'elevacion_descarga': 'int64','nivel_elevacion_locacion_mts': 'float64','radio_locacion': 'float64','id_material': 'float64',
    'elevacion_poligono_mts': 'float64','densidad_poligono': 'float64','tonelaje_inicial_poligono': 'float64','id_pases': 'float64',
    'id_palas_pases': 'float64','angulo_giro_promedio_pases': 'float64','has_block_pases': 'bool','capacidad_pes_equipo_carguio': 'float64', 'capacidad_vol_equipo_carguio': 'float64', 'tonelaje': 'int64',
    'radiohexagonocuchara_equipo_carguio': 'int64' }

# 6.2 Convertir las columnas al tipo de dato correspondiente
for columna, tipo in tipos_de_datos.items():
    datos[columna] = datos[columna].astype(tipo)

# 7. Transformacion de variables con diferente dimension de datos:

# #pasar tonelajevims a toneladas
datos['tonelajevims'] = datos['tonelajevims'] / 10


# #pasar efhcargado de centimetros a metros
# datos['efhcargado'] = datos['efhcargado'] / 100

# #pasar efhvacio de centimetros a metros
# datos['efhvacio'] = datos['efhvacio'] / 100

# #pasar distrealcargado de centimetros a metros
# datos['distrealcargado'] = datos['distrealcargado'] / 100

# #pasar distrealvacio de centimetros a metros
# datos['distrealvacio'] = datos['distrealvacio'] / 100

# #pasar coorxdesc de centimetros a Km
# datos['coorxdesc'] = datos['coorxdesc'] / 100000

# #pasar coorydesc de centimetros a Km
# datos['coorydesc'] = datos['coorydesc'] / 100000

# #pasar coorzdesc de centimetros a Km
# datos['coorzdesc'] = datos['coorzdesc'] / 100000

# #pasar velocidadvimscargado a  Km/hr
# datos['velocidadvimscargado'] = datos['velocidadvimscargado'] / 1000

# #pasar velocidadvimsvacio a  Km/hr
# datos['velocidadvimsvacio'] = datos['velocidadvimsvacio'] / 1000

# #pasar velocidadvimsvacio a  Km/hr
# datos['velocidadgpscargado'] = datos['velocidadgpscargado'] / 1000

# #pasar velocidadvimsvacio a  Km/hr
# datos['velocidadgpsvacio'] = datos['velocidadgpsvacio'] / 1000

# #pasar tonelajevimsretain a  toneladas
# datos['tonelajevimsretain'] = datos['tonelajevimsretain'] / 10

# #pasar coorzniveldescarga de centimetros a metros
# datos['coorzniveldescarga'] = datos['coorzniveldescarga'] / 100

# #pasar ancho_equipo de centimetros a metros
# datos['ancho_equipo'] = datos['ancho_equipo'] / 100

# #pasar largo_equipo de centimetros a metros
# datos['largo_equipo'] = datos['largo_equipo'] / 100

# #pasar tonelajevvanterior a toneladas
# datos['tonelajevvanterior'] = datos['tonelajevvanterior'] / 10

# #pasar tonelajevvposterior a toneladas
# datos['tonelajevvposterior'] = datos['tonelajevvposterior'] / 10

# 8. Transformacion de datos, convercion de Tipo de datos a Booleano (True/False)
# VARIABLE 1 
# 8.1 Reemplazar '0' por la moda
datos['termino_carga_equipo_en_espera_cuadrado_cuadrandose_carguio'] = datos['termino_carga_equipo_en_espera_cuadrado_cuadrandose_carguio'].replace(0,  datos['termino_carga_equipo_en_espera_cuadrado_cuadrandose_carguio'].mode().iloc[0])
# 8.2 Reemplazar 'True' por True y 'False' por False , Paso crucial para antes de Convertir a DATOS BOOLEANO
datos['termino_carga_equipo_en_espera_cuadrado_cuadrandose_carguio'] = datos['termino_carga_equipo_en_espera_cuadrado_cuadrandose_carguio'].replace({'True': True, 'False': False})
# 8.3 Convertir la columna a tipo de datos booleano
datos['termino_carga_equipo_en_espera_cuadrado_cuadrandose_carguio'] = datos['termino_carga_equipo_en_espera_cuadrado_cuadrandose_carguio'].astype(bool)

# VARIABLE 2
# 8.4 Reemplazar '0' por la moda
datos['cambio_estado_operatividad_carguio'] =datos['cambio_estado_operatividad_carguio'].replace(0, datos['cambio_estado_operatividad_carguio'].mode().iloc[0])

# 8.5 Reemplazar 'True' por True y 'False' por False , Paso crucial para antes de Convertir a DATOS BOOLEANO
datos['cambio_estado_operatividad_carguio'] = datos['cambio_estado_operatividad_carguio'].replace({'True': True, 'False': False})

# 8.6 Convertir la columna a tipo de datos booleano
datos['cambio_estado_operatividad_carguio'] = datos['cambio_estado_operatividad_carguio'].astype(bool)


# 9. Renombrar variables para mayor entendimiento del negocio
# Define un diccionario con los nuevos nombres de las columnas solo para algunas columnas
nuevos_nombres = {'id_cargadescarga' : 'id_cargadescarga_ciclo',
    'termino_carga_equipo_en_espera_cuadrado_cuadrandose_carguio' : 'al_termino_cargar_en_espera_cuadrado_cuadrandose',
    'id_descarga' : 'id_zona_hace_descarga',  
    'tiem_llegada_global': 'tiempo_llegada_camion', 'tiem_esperando': 'tiempo_esperando_camion_en_locacion', 
    'tiem_cuadra': 'tiempo_cuadra_camion','tiem_cuadrado': 'tiempo_cuadrado_camion' , 'tiem_carga' : 'tiempo_cargar_al_camion', 
    'tiem_acarreo' : 'tiempo_acarreo_camion', 'tiem_cola': 'tiempo_cola_camion_en_zonadescarga','tiem_retro': 'tiempo_retroceso_para_descargar',
    'tiem_listo' : 'tiempo_listo_para_descargar',  'tiem_descarga': 'tiempo_descarga_camion', 'tiem_viajando': 'tiempo_viajando_vacio_locacion', 
    'tonelaje':'tonelaje_nominal', 'tonelajevims':'tonelaje_segun_computadora', 'yn_estado': 'cambios_estado_en_ciclo',
    'distrealcargado': 'distancia_recorrida_camioncargado_km_gps_mts', 'distrealvacio': 'distancia_recorrida_camionvacio_km_gps_mts' ,
    'coorxdesc': 'coordenada_x_descarga_km', 'coorydesc': 'coordenada_y_descarga_km' , 'coorzdesc': 'coordenada_z_descarga_km',
    'tipodescargaidentifier': 'tipo_descarga_efectuado', 
    'tonelajevvanterior': 'tonelaje_camion_viajevacio_cicloanterior_vims', 'tonelajevvposterior': 'tonelaje_camion_viajevacio_cicloactual_vims',
    'velocidadvimscargado': 'promedio_velocidad_camioncargado_km/hr_compu', 
    'velocidadvimsvacio': 'promedio_velocidad_camionvacio_km/hr_compu',
    'velocidadgpscargado':'promedio_velocidad_camioncargado_km/hr_gps', 'velocidadgpsvacio': 'promedio_velocidad_camionvacio_km/hr_gps', 
    'tonelajevimsretain': 'tonelaje_camion_antes_cargaestabilizada', 'nivelcombuscargado': 'porcentaje_combustible_camioncargando', 
    'nivelcombusdescargado':'porcentaje_combustible_camiondescargando', 'volumen': 'volumen_nominal', 'aplicafactor_vol': 'aplica_factor_volumen_o_tonelaje',
    'coorzniveldescarga': 'nivel_descarga_metros', 'nombre_equipo':'nombre_equipo_acarreo', 
    'id_secundario':'id_flota_secundaria', 'flota_secundaria':'nombre_flota_secundaria', 'id_principal': 'id_flota_principal', 'flota_principal':'nombre_flota_principal',
    'capacidad_vol_equipo': 'capacidad_en_volumen_equipo_acarreo_m3', 'capacidad_pes_equipo':'capacidad_en_peso_equipo_acarreo', 'capacidadtanque_equipo': 'capacidadtanque_equipoacarreo_galones',
    'peso_bruto_equipo':'peso_bruto_equipo_acarreo', 'ishp_equipo':'si_no_equipo_altaprecision', 'ancho_equipo':'ancho_equipo_metros', 'largo_equipo':'largo_equipo_metros',
    'elevacion_descarga':'nivel_elevacion_descarga_metros', 'nombre_descarga':'nombre_zona_descarga', 
    'nombre_carga_locacion':'nombre_locacion_carga', 'nivel_elevacion_locacion_mts':'nivel_elevacion_locacion_carga_metros', 'radio_locacion':'radio_locacion_metros',
    'ids_poligonos_en_locacion':'ids_poligonos_en_locacion_carga', 'id_material': 'id_material_dominante_en_poligono', 
    'elevacion_poligono_mts':'elevacion_poligono_metros', 'ley_in':'lista_leyes', 'densidad_poligono':'densidad_inicial_poligono_creado_tn/m3',
    'capacidad_vol_equipo_carguio' : 'capacidad_en_volumen_equipo_carguio_m3', 'capacidad_pes_equipo_carguio':'capacidad_en_peso_equipo_carguio', 'capacidadtanque_equipo_carguio': 'capacidadtanque_equipocarguio_galones',
    'radiohexagonocuchara_equipo_carguio' : 'radiohexagonocuchara_equipocarguio', 'id_tablegen' : 'id_guardia_acarreocarga', 'nombre_tablegen' : 'nombre_guardia_acarreocarga', 
    'id_guardiadescarga': 'id_guardia_acarreodescarga', 'nombre_guardiadescarga':'nombre_guardia_acarreodescarga',
    'id':'id_guardia_carguio', 'nombre': 'nombre_guardia_carguio', 'id_locacion' : 'id_locacion_hace_carga','tonelaje_inicial_poligono': 'tonelaje_inicial_poligono_creado',  'efhvacio':'efhvacio_mts', 'efhcargado':'efhcargado_mts'
}
# 9.1 Renombra las columnas del DataFrame
datos = datos.rename(columns=nuevos_nombres)

#10. Agregamos Nuevas variables calculadas
#Agregamos la variable 'porcentaje_eficiencia_toneladas_movidas_acarreo'
datos['porcentaje_eficiencia_toneladas_movidas_acarreo'] = (datos['tonelaje_segun_computadora'] / datos['tonelaje_nominal']) * 100

#Agregamos la variable 'altura_elevacion'
datos['altura_elevacion'] = abs(datos['nivel_elevacion_descarga_metros'] - datos['nivel_elevacion_locacion_carga_metros'] )

# Agregamos la variable 'factor_perfil_rutavacio_mts'
datos['factor_perfil_rutavacio'] = np.where(datos['distancia_recorrida_camionvacio_km_gps_mts'] != 0,
                                            datos['efhvacio_mts'] / datos['distancia_recorrida_camionvacio_km_gps_mts'],
                                            0)

# Agregamos la variable 'factor_perfil_rutacargado_mts'
datos['factor_perfil_rutacargado'] = np.where(datos['distancia_recorrida_camioncargado_km_gps_mts'] != 0,
                                              datos['efhcargado_mts'] / datos['distancia_recorrida_camioncargado_km_gps_mts'],
                                              0)

# Agregamos la variable calculada "numero_pases_carguio" basado en la columna 'coord_x_pases'
datos['numero_pases_carguio'] = datos['coord_x_pases'].apply(lambda x: len(eval(x)) if isinstance(x, str) and '[' in x else x if isinstance(x, int) else 0)

#Agregamos la variable Galones_diponible_camioncargando
datos['Galones_disponibles_camioncargando'] = (datos['porcentaje_combustible_camioncargando']/100) * datos['capacidadtanque_equipoacarreo_galones']
#datos['demanda_galones_camioncargando'] = (datos['porcentaje_combustible_camioncargando']/100) * datos['capacidadtanque_equipoacarreo_galones']

#Agregar la variable Galones_diponible_camiondescargando 
datos['Galones_disponibles_camiondescargando'] = (datos['porcentaje_combustible_camiondescargando']/100) * datos['capacidadtanque_equipoacarreo_galones']

#Agregar la variable Galones_consumidos_entre_cargando_descargando 
datos['Galones_consumidos_entre_cargando_descargando_acarreo'] = datos['Galones_disponibles_camioncargando'] - datos['Galones_disponibles_camiondescargando']


# 12. Tratamiento de Valores Outliers (Numero de Pases - Variable Target )
# Iterar a través de las columnas numéricas y limitar los valores atípicos
# Calcular el Q1 y Q3 de la columna
# Q1 = datos['numero_pases_carguio'].quantile(0.25)
# Q3 = datos['numero_pases_carguio'].quantile(0.75)

# # Calcular el rango intercuartilico (IQR)
# IQR = Q3 - Q1

# # Calcular los límites inferior y superior del bigote
# Limite_inferior = round(Q1 - 1.5 * IQR)
# Limite_superior = round(Q3 + 1.5 * IQR)

# # Limitar los valores atípicos a los límites del bigote en la columna
# datos['numero_pases_carguio'] = datos['numero_pases_carguio'].clip(lower=Limite_inferior, upper=Limite_superior)

# n.1 Crear la base de datos si no existe en el almacenamiento de processed (en la ruta donde se almacenaran los datos preprocesados)
# spark.sql("CREATE DATABASE IF NOT EXISTS proyectopases_processed LOCATION '/mnt/datalakemlopsd4m/processed/proyectopases_processed/'")


# 11. Agregar mas filtros 
#11.1 Filtramos la fecha
fecha_minima = '2020-01-01 00:00:00.999'
datos=datos.sort_values(by='tiempo_inicio_carga_carguio') 
datos=datos[datos['tiempo_inicio_carga_carguio'] > fecha_minima]

#11.2 Definimos la funcion para hallar la procedencia de los minerales
def definir_material(dato):
    regirstro = dato[0:2]
    material = 'Definido'
    if regirstro == 'OX':
        material = 'OXI'
    elif regirstro == 'SU':
        material = 'SUL'
    elif regirstro == 'DE':
        material = 'NoDefinido'
    elif regirstro == 'RI':
        material = 'NoDefinido'
    elif regirstro == 'na':
        material = 'NoDefinido'
    elif isinstance(eval(regirstro[0:1]), int):
        material = 'TAJ'
    else:
        material = 'NoDefinido'
    return material

# 11.3 Hacemos el procesamiento de los datos apartir del nombre del poligono
tabla_datos = datos['nombre_poligono'].str.split('-', n=4,expand=True)
 
name_columns = {0:'Tajo', 1:'Zona', 2:'Nivel', 3:'Poligono',4:'Material'}
tabla_datos =  tabla_datos.rename(columns=name_columns)

# 11.4 Convertir todos los valores de 'nombre_poligono' a cadenas de texto
tabla_datos['Poligono'] = tabla_datos['Poligono'].astype(str)
 
tabla_datos['Procedencia'] = tabla_datos['Poligono'].apply(lambda a: definir_material(a))

datos = pd.concat([datos,tabla_datos], axis=1)

# Calculo tonelaje por pase
tabla_datos2 = datos['tonelaje_pases'].str.strip('[]').str.split(',', expand=True)
for columna in tabla_datos2.columns:
    tabla_datos2[columna] = tabla_datos2[columna].astype('float')
datos['tonelaje_pases'] = tabla_datos2.sum(axis=1)/10


# Filtro 
# 12. Tratamiento de Valores Outliers (Numero de Pases)
mask = datos['numero_pases_carguio'] > 0
datos = datos[mask]
 
# Filtramos los tonelajes Vims (70% * capacidad de tolva) , para tener ciclos con pases 
mask = (datos['tonelaje_segun_computadora'] > 150)
datos = datos[mask]
 
# Filtramos los tonelajes por pases, que sean distintos de 0, en cada pase que se dio 
mask = datos['tonelaje_pases'] != 0
datos = datos[mask]
 
# Filtrar las tonaledas por pase. que los pases dados, sean superioes a (70%capacidad tolva) y menores al (125%capacidad tolva)
mask = (datos['tonelaje_pases'] >= 180) & (datos['tonelaje_pases'] <= 400)
datos = datos[mask]
 
# Filtramos la Procedencia de Material diferente de DES 
mask = datos['Procedencia'] != 'DES'
datos = datos[mask]
 
# Filtramos solo pases por encima de 4, y los configuramos con el tonelaje Vims 
mask = datos['numero_pases_carguio'] < 4
datos.loc[mask, 'numero_pases_carguio'] = (datos[mask]['tonelaje_segun_computadora'] / datos[mask]['capacidad_en_peso_equipo_carguio']).round(0)

# Cambiar 'DELAY' a 'READY' donde 'numero_pases_carguio' es diferente de 0
datos.loc[(datos['estado_primario_pala'] == 'DELAY') & (datos['numero_pases_carguio'] != 0), 'estado_primario_pala'] = 'READY'
# Filtramos solos los Readys(donde se efectuaron pases)
datos = datos[datos['estado_primario_pala'] == 'READY']

# Agregamos la variable 'tiempo de carga', cuanto demora el operador de carguio en realizar el carguio al camion de acarreo
datos['tiempo_carga'] = (datos['tiempo_esperando_carguio'] - datos['tiempo_inicio_carga_carguio']).dt.total_seconds() #Pasas a segundos


#Convertir la variable object a Numerica, para poder realizar la Regresion y el reporte estadistco de Hipotesis Nula
# datos['radiohexagonocuchara_equipocarguio']=datos['radiohexagonocuchara_equipocarguio'].astype('int64')

# n Antes de Guardar convertir el df-pandas Preprocesado a un DataFrame de Spark
spark_datos = spark.createDataFrame(datos)

# Listar todas las bases de datos
# spark.sql("SHOW DATABASES").show(truncate=False )
# n.1 Crear la base de datos si no existe en el almacenamiento de processed (en la ruta donde se almacenaran los datos preprocesados)
# spark.sql("CREATE DATABASE IF NOT EXISTS dbproyectocongestion_processed LOCATION '/mnt/datalakemlopsd4m/processed/proyectocongestion_processed/'")

# n.1 Guardamos los datos preprocesados en el Storage de Processed , en una Tabla DELTA
#spark_datos.write.format("delta").mode("overwrite").saveAsTable("processed_db.datmarcobre_prepro2_delta") #processed_db:Nombre de BD, datos_processed_delta:Nombre
spark_datos.write.format("delta").mode("overwrite").saveAsTable("proyectopases_presentation.datos_presentation_tabladelta_bi") #proyectopases_processed:Nombre_BD_creada