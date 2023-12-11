from loguru import logger
import functools
import time

# Configuração básica do logger
logger.add("logs/data_warehouse_log_{time}.log", rotation="1 week")

def log(func):
    """
    Decorador para adicionar logs automáticos a uma função, incluindo o tempo de início, fim e a duração.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()  # Tempo de início
        logger.info(f"Iniciando a função '{func.__name__}' às {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")
        try:
            result = func(*args, **kwargs)
            end_time = time.time()  # Tempo de fim
            duration = end_time - start_time  # Duração do processo
            logger.success(f"A função '{func.__name__}' completou com sucesso em {duration:.2f} segundos")
            return result
        except Exception as e:
            logger.error(f"A função '{func.__name__}' falhou com a exceção: {e}")
            raise

    return wrapper
