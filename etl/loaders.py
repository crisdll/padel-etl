from psycopg2.extras import execute_values
import logging

# Obtener el logger configurado
logger = logging.getLogger('padel_etl')

def load_categorias(cursor, connection, categorias):
    if categorias.empty:
        logger.warning("No hay categorías para insertar.")
        return

    sql = """
    INSERT INTO categorias (categoria_api_id, nombre, genero, fecha)
    VALUES %s
    ON CONFLICT (categoria_api_id) DO UPDATE
    SET fecha = EXCLUDED.fecha;
    """

    values = [tuple(row) for row in categorias.values]
    logger.info(f"Iniciando carga de {len(values)} categorías")

    try:
        execute_values(cursor, sql, values)
        connection.commit()
        logger.info(f"✅ Procesadas {len(values)} categorías (insertadas/actualizadas).")
    except Exception as e:
        connection.rollback()
        logger.error(f"❌ Error insertando categorías: {e}")
        logger.debug(f"SQL que falló: {sql}")
        logger.debug(f"Primeros 3 valores: {values[:3]}")
        raise  # Re-lanzar la excepción para que se maneje arriba

def load_clubs(cursor, connection, clubs):
    if clubs.empty:
        logger.warning("No hay clubes para insertar.")
        return

    sql = """
    INSERT INTO clubs (nombre, categoria_api_id, fecha)
    VALUES %s
    ON CONFLICT (nombre, categoria_api_id) DO UPDATE
    SET fecha = EXCLUDED.fecha;
    """

    values = [tuple(row) for row in clubs.values]
    logger.info(f"Iniciando carga de {len(values)} clubs")

    try:
        execute_values(cursor, sql, values)
        connection.commit()
        logger.info(f"✅ Procesados {len(values)} clubes (insertados/actualizados).")
    except Exception as e:
        connection.rollback()
        logger.error(f"❌ Error insertando clubes: {e}")
        logger.debug(f"SQL que falló: {sql}")
        logger.debug(f"Primeros 3 valores: {values[:3]}")
        raise

def load_enfrentamientos(cursor, connection, enfrentamientos):
    if enfrentamientos.empty:
        logger.warning("No hay enfrentamientos para insertar.")
        return
    
    # Log detallado para debug
    logger.info(f"Iniciando carga de {len(enfrentamientos)} enfrentamientos")
    logger.debug(f"Columnas del DataFrame: {list(enfrentamientos.columns)}")
    logger.debug(f"Tipos de datos: {enfrentamientos.dtypes.to_dict()}")
    
    sql = """
    INSERT INTO enfrentamientos (fecha_partido, resultado, club_local_id, club_visitante_id, jornada, enfrentamiento_api_id, fecha, categoria_api_id)
    VALUES %s
    ON CONFLICT (enfrentamiento_api_id) DO UPDATE
    SET club_local_id = EXCLUDED.club_local_id,
        club_visitante_id = EXCLUDED.club_visitante_id,
        fecha = EXCLUDED.fecha,
        fecha_partido = EXCLUDED.fecha_partido,
        resultado = EXCLUDED.resultado;
    """

    values = [tuple(row) for row in enfrentamientos.values]
    logger.debug(f"Primer registro a insertar: {values[0] if values else 'Sin datos'}")

    try:
        execute_values(cursor, sql, values)
        connection.commit()
        logger.info(f"✅ Procesados {len(values)} enfrentamientos (insertados/actualizados).")
    except Exception as e:
        connection.rollback()
        logger.error(f"❌ Error insertando enfrentamientos: {e}")
        logger.debug(f"SQL que falló: {sql}")
        logger.debug(f"Primeros 3 valores: {values[:3]}")
        # Log adicional para enfrentamientos por ser más complejo
        logger.error(f"Orden de columnas esperado: fecha_partido, resultado, club_local_id, club_visitante_id, jornada, enfrentamiento_api_id, fecha, categoria_api_id")
        logger.error(f"Orden de columnas en DataFrame: {list(enfrentamientos.columns)}")
        raise

def load_resultados(cursor, connection, resultados):
    if resultados.empty:
        logger.warning("No hay resultados para insertar.")
        return
    
    logger.info(f"Iniciando carga de {len(resultados)} resultados")
    
    sql = """
    INSERT INTO resultados (partido_api_id, is_local_ganador, nombre1_local, nombre2_local, nombre1_visitante, nombre2_visitante, set1_local, set1_visitante, set2_local, set2_visitante, set3_local, set3_visitante, pista, puntos, fecha, enfrentamiento_api_id)
    VALUES %s
    ON CONFLICT (partido_api_id) DO UPDATE
    SET is_local_ganador = EXCLUDED.is_local_ganador,
        nombre1_local = EXCLUDED.nombre1_local,
        nombre2_local = EXCLUDED.nombre2_local,
        nombre1_visitante = EXCLUDED.nombre1_visitante,
        nombre2_visitante = EXCLUDED.nombre2_visitante,
        set1_local = EXCLUDED.set1_local,
        set1_visitante = EXCLUDED.set1_visitante,
        set2_local = EXCLUDED.set2_local,
        set2_visitante = EXCLUDED.set2_visitante,
        set3_local = EXCLUDED.set3_local,
        set3_visitante = EXCLUDED.set3_visitante,
        pista = EXCLUDED.pista,
        puntos = EXCLUDED.puntos,
        fecha = EXCLUDED.fecha,
        enfrentamiento_api_id = EXCLUDED.enfrentamiento_api_id;
    """

    values = [tuple(row) for row in resultados.values]

    try:
        execute_values(cursor, sql, values)
        connection.commit()
        logger.info(f"✅ Procesados {len(values)} resultados (insertados/actualizados).")
    except Exception as e:
        connection.rollback()
        logger.error(f"❌ Error insertando resultados: {e}")
        logger.debug(f"SQL que falló: {sql}")
        logger.debug(f"Primeros 3 valores: {values[:3]}")
        raise