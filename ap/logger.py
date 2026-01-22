import logging
from ap.config import Config

def get_logger(name: str) -> logging.Logger:
    cfg = Config()
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(cfg.LOG_LEVEL)
    h = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    h.setFormatter(fmt)
    logger.addHandler(h)
    logger.propagate = False
    return logger

