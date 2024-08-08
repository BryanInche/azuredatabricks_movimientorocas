import psycopg2
import pandas as pd

def consultar_postgres():
    # Información de la conexión a PostgreSQL
    host = "192.168.5.114"
    database = "ControlSenseDB"
    user = "postgres"
    password = "larc0mar"

    # Establecer la conexión a la base de datos
    try:
        connection = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )

        # Crear un cursor para ejecutar comandos SQL
        cursor = connection.cursor()

        # Establecer la zona horaria antes de ejecutar la consulta
        time_zone_query = "SET TIME ZONE 'America/Lima';"
        cursor.execute(time_zone_query)

        # Tu consulta SQL
        tu_query_sql = '''
        select * from public.tp_cargadescarga
        limit 10
        '''
        # Ejecutar la consulta
        cursor.execute(tu_query_sql)

        # Obtener los resultados en un DataFrame de pandas
        resultados_df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

        # Cerrar el cursor y la conexión
        cursor.close()
        connection.close()

        # Devolver el DataFrame con los resultados
        return resultados_df

    except psycopg2.Error as e:
        print("Error al conectar a la base de datos PostgreSQL:", e)
        return None
    
datos = consultar_postgres()
datos.head()