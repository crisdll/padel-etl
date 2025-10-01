import psycopg2
import pandas as pd
import logging
import sys
from config import USER, PASSWORD, HOST, PORT, DBNAME
from etl.api_client import get_competition_id,get_categorias, get_clubs, get_enfrentamientos, get_resultados
from etl.loaders import load_categorias, load_clubs, load_enfrentamientos, load_resultados
from logger_config import setup_logger


def conect_to_supabase():
    logger = logging.getLogger('padel_etl')
    try:
        conn = psycopg2.connect(user=USER, password=PASSWORD, host=HOST, port=PORT, dbname=DBNAME)
        logger.info("✅ Conexión exitosa a Supabase")
        return conn
    except Exception as e:
        logger.error(f"❌ Error al conectar: {e}")
        return None  # ✅ Retornar None en lugar de sys.exit aquí


def main():
    logger = setup_logger(logging.INFO)
    success = False  # ✅ Flag para controlar el exit code

    try:
        # =======================================================
        # STEP 1: EXTRACT FROM API AND TRANSFORM DATA
        # =======================================================
        logger.info("🔄 Iniciando extracción de datos de la API...")

        #1. Get current competition ID
        logger.info("Obteniendo ID de competición...")
        id, id_base64 = get_competition_id("lliga14")
        logger.info(f"ID: {id}, ID_BASE64: {id_base64}")

        #TEST - TO REMOVE
        #id = 308
        #id_base64 = 'MzA4'

        #2. Get categories
        logger.info("📂 Obteniendo categorías...")
        df_categorias = get_categorias(id_base64)
        
        #3. Get clubs and enfrentamientos for all categories
        logger.info("🏃‍♂️ Obteniendo clubs y enfrentamientos...")
        df_clubs = pd.DataFrame()
        df_enfrentamientos = pd.DataFrame()
        
        for index, row in df_categorias.iterrows():
            selected_category = row['categoria_api_id']
            logger.info(f"   Procesando categoría: {selected_category}")

            # Get clubs of the category
            clubs_data = get_clubs(id, selected_category)
            df_clubs = pd.concat([df_clubs, clubs_data], ignore_index=True)
            
            # Get enfrentamientos of the category
            enfrentamientos_data = get_enfrentamientos(id_base64, selected_category)
            df_enfrentamientos = pd.concat([df_enfrentamientos, enfrentamientos_data], ignore_index=True)

        #4. Get resultados for enfrentamientos with results
        logger.info("🏆 Obteniendo resultados...")
        enfrentamientos_con_resultado = df_enfrentamientos[
            ~df_enfrentamientos['resultado'].str.replace(" ", "").str.lower().str.contains("sinresultado", na=False)
        ]
        
        df_resultados = pd.DataFrame()
        for index, row in enfrentamientos_con_resultado.iterrows():
            logger.info(f"   Obteniendo resultados para enfrentamiento: {row['enfrentamiento_api_id']}")
            resultados_data = get_resultados(row['enfrentamiento_api_id'])
            df_resultados = pd.concat([df_resultados, resultados_data], ignore_index=True)

        # Resumen de datos extraídos
        logger.info("📊 RESUMEN DE DATOS EXTRAÍDOS:")
        logger.info(f"   Categorías: {len(df_categorias)}")
        logger.info(f"   Clubs: {len(df_clubs)}")
        logger.info(f"   Enfrentamientos: {len(df_enfrentamientos)}")
        logger.info(f"   Resultados: {len(df_resultados)}")

        # =======================================================
        # STEP 2: DATA LOAD TO SUPABASE
        # =======================================================
        logger.info("\n💾 Iniciando carga de datos a Supabase...")
        
        conn = conect_to_supabase()
        if not conn:
            logger.error("❌ No se pudo conectar a Supabase. Terminando proceso.")
            raise Exception("Error de conexión a Supabase")  # ✅ Usar raise en lugar de sys.exit

        try:
            with conn.cursor() as cur:
                # Cargar datos en orden de dependencias
                logger.info("📂 Cargando categorías...")
                load_categorias(cur, conn, df_categorias)

                logger.info("🏃‍♂️ Cargando clubs...")
                load_clubs(cur, conn, df_clubs)

                logger.info("🤝 Cargando enfrentamientos...")
                load_enfrentamientos(cur, conn, df_enfrentamientos)

                logger.info("🏆 Cargando resultados...")
                load_resultados(cur, conn, df_resultados)

            # Commit final
            conn.commit()
            logger.info("✅ Todos los datos cargados exitosamente")
            success = True  # ✅ Marcar como exitoso
            
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Error durante la carga: {e}")
            raise  # ✅ Re-lanzar la excepción
        finally:
            conn.close()
            logger.info("🔐 Conexión cerrada")

        logger.info("✅ Proceso ETL completado exitosamente")
        
    except Exception as e:
        logger.error(f"❌ Error crítico en el proceso ETL: {e}")
        logger.exception("Detalles del error:")
        success = False  # ✅ Marcar como fallido
    
    finally:
        logger.info("=" * 50)
        if success:
            logger.info("FIN DEL PROCESO ETL - ÉXITO")
            logger.info("=" * 50)
            sys.exit(0)  # ✅ Exit exitoso
        else:
            logger.info("FIN DEL PROCESO ETL - ERROR")
            logger.info("=" * 50)
            sys.exit(1)  # ✅ Exit con error


if __name__ == "__main__":
    main()