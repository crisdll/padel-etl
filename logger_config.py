import logging
import os
from datetime import datetime

def setup_logger(log_level=logging.INFO):
    """
    Configure the logging system for the Padel ETL process.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)

    Returns:
        logger: Configured logger instance
    """
    
    # Crear directorio 'logs' si no existe
    logs_dir = os.path.join(os.path.dirname(__file__), 'logs')
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    
    # Name of the log file with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f'padel_etl_{timestamp}.log'
    log_filepath = os.path.join(logs_dir, log_filename)
    
    # Personalization of the logging format
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    
    # File Handler for detailed logs
    file_handler = logging.FileHandler(log_filepath, encoding='utf-8')
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    
    # Console Handler for summary logs
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    
    # Primary Logger Configuration
    logger = logging.getLogger('padel_etl')
    logger.setLevel(log_level)
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # Avoid log propagation to root logger
    logger.propagate = False
    
    # Initial log messages
    logger.info("=" * 50)
    logger.info("INICIO DEL PROCESO ETL PADEL VALLES")
    logger.info("=" * 50)
    logger.info(f"Log guardado en: {log_filepath}")
    logger.info(f"Nivel de logging: {logging.getLevelName(log_level)}")
    
    return logger