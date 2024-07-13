import psycopg2
import pandas as pd

def consultar_postgres_y_obtener_df():
    # Información de la conexión a PostgreSQL
    host = "controlsenesedb.postgres.database.azure.com"
    database = "ControlSenseDB"
    user = "postgres"
    password = "Larc0mar"

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
        select z.* from (select distinct a.id id_ciclo_acarreo, a.id_cargadescarga, a.id_palas, a.id_equipo id_equipo_camion,  
        m.id_ciclo_carguio,	m.id_equipo_carguio , m.id_trabajador id_trabajador_pala, m.id_crew id_guardia_realiza_carga_al_camion,
        m.id_locacion id_locacion, 
        m.id_poligono id_poligono_se_obtiene_material, m.tiempo_inicio_carga_carguio, m.tiempo_esperando_carguio, 
        m.tiempo_ready_cargando tiempo_ready_cargando_pala, m.tiempo_ready_esperando tiempo_ready_esperando_pala,
        m.previous_esperando_pala, m.isspot termino_carga_equipo_en_espera_cuadrado_cuadrandose_carguio, m.bool_estado cambio_estado_operatividad_carguio,
        m.bool_equipo_next tiene_camion_cuadrado_al_termino_carga_pala, m.cola cantidad_equipos_espera_al_termino_carga_pala,
        s.id_estados id_estados_camion, s.id_equipo id_equipo_table_estados_camion, s.id_detal_estado id_detal_estado_camion, s.tiempo_inicio_cambio_estado tiempo_inicio_cambio_estado_camion,
        s.tiempo_estimado_duracion_estado tiempo_estimado_duracion_estado_camion,
        s.en_campo_o_taller_mantenimiento en_campo_o_taller_mantenimiento_camion, s.tipoubicacionsupervisor tipoubicacionsupervisor_camion,
        s.id_tipo_estad id_tipo_estad_camion, s.estado_detalle estado_detalle_camion, s.estado_secundario estado_secundario_camion, 				 
        s.estado_primario estado_primario_camion,
        ss.id_estados id_estados_pala, ss.id_equipo id_equipo_table_estados_pala, ss.id_detal_estado id_detal_estado_pala, 
        ss.tiempo_inicio_cambio_estado tiempo_inicio_cambio_estado_pala, ss.tiempo_estimado_duracion_estado tiempo_estimado_duracion_estado_pala,
        ss.en_campo_o_taller_mantenimiento en_campo_o_taller_mantenimiento_pala, ss.tipoubicacionsupervisor tipoubicacionsupervisor_pala, 
        ss.id_tipo_estad id_tipo_estad_pala, ss.estado_detalle estado_detalle_pala, ss.estado_secundario estado_secundario_pala, 				 
        ss.estado_primario estado_primario_pala,
        a.id_descarga, a.id_factor, m.id_poligono, a.tiem_llegada tiem_llegada_global, a.tiem_esperando, a.tiem_cuadra, a.tiem_cuadrado,
        a.tiem_carga, a.tiem_acarreo, a.tiem_cola, a.tiem_retro, a.tiem_listo,a.tiem_descarga, a.tiem_viajando,
        a.id_trabajador id_trabajador_camion, a.id_palanext, a.tonelaje, a.tonelajevims, 
        a.id_mezcla, a.yn_estado, a.yn_operador, a.id_crewcarga id_guardia_hizocarga, a.id_crewdescarga id_guardia_hizodescarga,  
        b.id_zona id_zona_aplicafactor, o.id_zona id_zona_pertenece_poligono, b.factor,
        (a.tonelajevims/10) * (b.factor/1000) toneladas_secas,
        (a.tonelajevims / 10.0) / NULLIF(((m.tiempo_ready_cargando + m.tiempo_ready_esperando) / 3600), 0)  productividad_operativa_carguio_tn_h,			 		 
        a.efhcargado, a.efhvacio, a.distrealcargado, a.distrealvacio, a.coorxdesc, a.coorydesc, a.coorzdesc, 
        a.tipodescargaidentifier, a.tonelajevvanterior, a.tonelajevvposterior, a.dumpreal, a.loadreal, a.velocidadvimscargado,
        a.velocidadvimsvacio, a.velocidadgpscargado, a.velocidadgpsvacio, a.tonelajevimsretain, a.nivelcombuscargado, a.nivelcombusdescargado,
        a.volumen, a.aplicafactor_vol, a.coorzniveldescarga,
        a.efh_factor_loaded, a.efh_factor_empty,f.nombre_equipo, f.id_secundario, f.secundario flota_secundaria, f.id_principal, f.principal flota_principal,
        f.capacidad_vol capacidad_vol_equipo, f.capacidad_pes capacidad_pes_equipo, f.capacidadtanque capacidadtanque_equipo, 
        f.fcorrec_efhod fcorrec_efhod_equipo, f.fcorrec_efhdo fcorrec_efhdo_equipo, f.pesobruto peso_bruto_equipo, f.ishp ishp_equipo,
        f.ancho ancho_equipo, f.largo largo_equipo, f.numeroejes numeroejes_equipo, f.tipoespecial tipoespecial_equipo, 
        f.radiohexagonoequipo radiohexagonoequipo_equipo, f.radiohexagonocuchara radiohexagonocuchara_equipo, 
        ff.nombre_equipo nombre_equipo_carguio,
        ff.capacidad_vol capacidad_vol_equipo_carguio, ff.capacidad_pes capacidad_pes_equipo_carguio, ff.capacidadtanque capacidadtanque_equipo_carguio, 
        ff.radiohexagonoequipo radiohexagonoequipo_carguio, ff.radiohexagonocuchara radiohexagonocuchara_equipo_carguio, 
        g.id_turnos id_turnos_turnocarga, g.nombre nombre_turnocarga, g.horaini horaini_turnocarga, g.horafin horafin_turnocarga,
        h.id_turnos id_turnos_turnodescarga, h.nombre nombre_turnodescarga, h.horaini horaini_turnodescarga, h.horafin horafin_turnodescarga,
        j.id_tajo id_zona_encuentra_descarga, n.id_nodo id_nodo_carga, p.nombre_nodo nombre_nodo_carga, p.idzona id_zona_nodo_carga,
        j.id_nodo id_nodo_descarga, q.nombre_nodo nombre_nodo_descarga, q.idzona id_zona_nodo_descarga, j.nivel elevacion_descarga, 
        j.nombre nombre_descarga, n.nombre nombre_carga_locacion, n.nivel nivel_elevacion_locacion_mts, n.radio radio_locacion, 
        n.poligono_ids ids_poligonos_en_locacion, o.id_material, o.nombre nombre_poligono, o.nivel elevacion_poligono_mts, o.ley_in, 
        o.densidad densidad_poligono, o.tonelaje_inicial tonelaje_inicial_poligono,
        t.id id_pases, 	t.id_palas id_palas_pases, 	t.id_cargadescarga id_cargadescarga_pases, t.coord_x 	coord_x_pases,
        t.coord_y coord_y_pases, t.coord_z coord_z_pases, t.angulo_giro angulo_giro_pases, t.tonelaje tonelaje_pases, 
        t.duracion_excavacion duracion_excavacion_pases, t.angulo_giro_promedio angulo_giro_promedio_pases, t.has_block has_block_pases,
        w.id id_guardia_acarreocarga, w.id_guardias id_guardias_acarreocarga, w.nombre nombre_guardia_acarreocarga,
        x.id id_guardia_acarreodescarga, x.id_guardias id_guardias_acarreodescarga, x.nombre nombre_guardia_acarreodescarga, 
        y.id id_guardia_carguio, y.id_guardias id_guardias_carguio, y.nombre nombre_guardia_carguio, 
        ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY a.id DESC) AS RowNum
        from public.tp_cargadescarga a
        left join public.ta_factortonelaje b
        on a.id_factor = b.id
        left join (select d.id_principal, d.principal, d.id_secundario, d.secundario, c.id id_equipo, c.nombre nombre_equipo, c.capacidad_vol,
        c.capacidad_pes, c.capacidadtanque, c.fcorrec_efhod, c.fcorrec_efhdo, c.pesobruto, c.ishp, c.ancho, c.largo,
        c.numeroejes, c.anho, c.tipoespecial, c.radiohexagonoequipo, c.radiohexagonocuchara
        from public.ts_equipos c
        inner join (select b.id id_principal , b.nombre principal, a.id id_secundario , a.nombre secundario from public.ts_equipos a
        inner join (select id, nombre from public.ts_equipos where id_flota = 0 and tiem_elimin is null) b
        on b.id = a.id_flota
        where a.tiem_elimin is null) d
        on d.id_secundario = c.id_flota
        where c.id_flota <> 0 and c.isflota = false and c.tiem_elimin is null
        order by d.id_principal) f
        on a.id_equipo = f.id_equipo
        left join (select id, id_turnos, nombre, horaini, horafin from public.ts_turnos
        where tiem_elimin is null) g
        on a.id_turnocarga = g.id
        left join (select id, id_turnos, nombre, horaini, horafin from public.ts_turnos
        where tiem_elimin is null) h
        on a.id_turnodescarga = h.id
        left join public.ts_descarga j 
        on a.id_descarga = j.id
        left join (
        SELECT tp.id id_ciclo_carguio, tp.id_palas, tp.id_equipo as id_equipo_carguio, 
        tp.id_locacion, 
        tp.id_poligono, tp.id_trabajador, tp.id_crew, tp.isspot, tp.bool_estado, tp.bool_equipo_next, tp.cola, 
        tcd1.tiem_carga as tiempo_inicio_carga_carguio,
        tcd1.tiem_acarreo as tiempo_esperando_carguio,
        getreadytime( tp.id_equipo, tcd1.tiem_carga, tcd1.tiem_acarreo) tiempo_ready_cargando,
        getreadytime( tp.id_equipo, 
                    lag(tcd1.tiem_acarreo) OVER (PARTITION BY (COALESCE(null, true)), tp.id_equipo ORDER BY tcd1.tiem_carga), 
                    tcd1.tiem_carga) tiempo_ready_esperando,
        lag(tcd1.tiem_acarreo) OVER (PARTITION BY (COALESCE(null, true)), tp.id_equipo ORDER BY tcd1.tiem_carga) AS previous_esperando_pala,
        tcd1.tiem_acarreo,
        tcd1.tiem_carga
        --tcd1.*
        FROM tp_cargadescarga tcd1
        LEFT JOIN tp_palas tp ON tp.id = (SELECT id 
                                FROM tp_palas 
                                    WHERE id_palas = tcd1.id_palas
                                    ORDER BY ID DESC LIMIT 1)
        WHERE tcd1.tiem_elimin IS NULL
        AND tcd1.tiem_viajando IS NOT NULL
        and tp.tiem_elimin IS NULL and tcd1.tiem_carga > CURRENT_TIMESTAMP - INTERVAL '1 year' ) m   --MODIFICAR FECHAS (1 mes atras, etc)
        on a.id_palas = m.id_palas
        left join public.ts_locacion n
        on m.id_locacion = n.id
        left join public.ts_poligono o
        on m.id_poligono = o.id
        left join (select * from public.tp_nodos
        where loc_carga = 'true' and tiem_elimin is null) p
        on n.id_nodo = p.id --id_nodo
        left join (select * from public.tp_nodos
        where loc_descarga = 'true' and tiem_elimin is null) q
        on j.id_nodo = q.id				 
        left join (select d.id_principal, d.principal, d.id_secundario, d.secundario, c.id id_equipo, c.nombre nombre_equipo, c.capacidad_vol,
        c.capacidad_pes, c.capacidadtanque, c.fcorrec_efhod, c.fcorrec_efhdo, c.pesobruto, c.ishp, c.ancho, c.largo,
        c.numeroejes, c.anho, c.tipoespecial, c.radiohexagonoequipo, c.radiohexagonocuchara
        from public.ts_equipos c
        inner join (select b.id id_principal , b.nombre principal, a.id id_secundario , a.nombre secundario from public.ts_equipos a
        inner join (select id, nombre from public.ts_equipos where id_flota = 0 and tiem_elimin is null) b
        on b.id = a.id_flota
        where a.tiem_elimin is null) d
        on d.id_secundario = c.id_flota
        where c.id_flota <> 0 and c.isflota = false and c.tiem_elimin is null
        order by d.id_principal) ff
        on m.id_equipo_carguio = ff.id_equipo		 
        left join (select z.* from
        (select A.id, A.id_estados, A.id_equipo, A.id_detal_estado, A.tiempo_inicio tiempo_inicio_cambio_estado,
        A.tiempo_estimado tiempo_estimado_duracion_estado, A.idenestado en_campo_o_taller_mantenimiento,
        A.tipoubicacionsupervisor,
        B.id_tipo_estad, B.nombre as estado_detalle,
        (select nombre from public.ts_detal_estado where id=B.id_tipo_estad limit 1) as estado_secundario, 
        (select nombre from public.ts_detal_estado where id = (select id_tipo_estad from public.ts_detal_estado where id=B.id_tipo_estad limit 1) limit 1) as estado_primario,
        ROW_NUMBER() OVER (PARTITION BY A.id_equipo ORDER BY A.id_equipo) AS row_num
        from public.tp_estados A
        left join public.ts_detal_estado B on A.id_detal_estado = B.id_detal_estado
        --left join (select * from public.ts_detal_estado where id=B.id_tipo_estad limit 1) C on true
        where A.tiem_elimin is null) z
        WHERE
        z.row_num = 1) s
        on a.id_equipo = s.id_equipo
        left join (select z.* from
            (select A.id, A.id_estados, A.id_equipo, A.id_detal_estado, A.tiempo_inicio tiempo_inicio_cambio_estado,
            A.tiempo_estimado tiempo_estimado_duracion_estado, A.idenestado en_campo_o_taller_mantenimiento,
            A.tipoubicacionsupervisor,
            B.id_tipo_estad, B.nombre as estado_detalle,
            (select nombre from public.ts_detal_estado where id=B.id_tipo_estad limit 1) as estado_secundario, 
            (select nombre from public.ts_detal_estado where id = (select id_tipo_estad from public.ts_detal_estado where id=B.id_tipo_estad limit 1) limit 1) as estado_primario,
            ROW_NUMBER() OVER (PARTITION BY A.id_equipo ORDER BY A.id_equipo) AS row_num
            from public.tp_estados A
            left join public.ts_detal_estado B on A.id_detal_estado = B.id_detal_estado
            --left join (select * from public.ts_detal_estado where id=B.id_tipo_estad limit 1) C on true
            where A.tiem_elimin is null) z
            WHERE
            z.row_num = 1) ss
        on m.id_equipo_carguio = ss.id_equipo
        left join (select * from public.ta_datacarga_sensores 
        where tiem_elimin is NULL) t
        on a.id_cargadescarga = t.id_cargadescarga
        left join (select * from public.ta_guardias) w
        on a.id_crewcarga = w.id
        left join (select * from public.ta_guardias) x
        on a.id_crewdescarga = x.id
        left join (select * from public.ta_guardias) y
        on m.id_crew = y.id
        where a.tiem_elimin is null and a.tiem_llegada > CURRENT_TIMESTAMP - INTERVAL '1 year'  and b.factor is not null ) z --and g.id_turnos= 1 and h.id_turnos = 1, --MODIFICAR FECHAS (1 mes atras, etc))
        -- year, month, etc
        where z.RowNum = 1
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