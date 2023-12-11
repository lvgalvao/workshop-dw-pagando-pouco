# utility_scripts/logging_utils.py
from loguru import logger
import functools

# Configuração básica do logger
logger.add("logs/data_warehouse_log_{time}.log", rotation="1 week")

def log(func):
    """
    Decorador para adicionar logs automáticos a uma função.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"Iniciando a função '{func.__name__}' com os argumentos {args} e {kwargs}")
        try:
            result = func(*args, **kwargs)
            logger.success(f"A função '{func.__name__}' completou com sucesso")
            return result
        except Exception as e:
            logger.error(f"A função '{func.__name__}' falhou com a exceção: {e}")
            raise

    return wrapper
