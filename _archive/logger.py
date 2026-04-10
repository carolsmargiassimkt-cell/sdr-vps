import logging
import os

LOG_PATH = "logs"
os.makedirs(LOG_PATH, exist_ok=True)

def setup_logger(nome="sdr"):

    logger = logging.getLogger(nome)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    # console
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)

    # arquivo
    fh = logging.FileHandler(f"{LOG_PATH}/sdr.log", encoding="utf-8")
    fh.setFormatter(formatter)

    logger.addHandler(ch)
    logger.addHandler(fh)

    return logger
