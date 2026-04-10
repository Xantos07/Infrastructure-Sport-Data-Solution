import logging


def get_logger(name: str) -> logging.Logger:
    """Retourne un logger configuré pour le service donné."""
    log = logging.getLogger(name)
    if not log.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))
        log.addHandler(handler)
        log.setLevel(logging.INFO)
        log.propagate = False
    return log


# Rétrocompatibilité — modules existants qui font `from config.logger import logger`
logger = get_logger("generate_ticket")
