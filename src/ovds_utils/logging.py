import logging
import os
import sys

LOG_LEVEL = os.environ.get("LOGLVL", "NOTSET")
FORMAT = "%(asctime)s::%(name)s::%(levelname)s::%(message)s"


def get_logger(name):
    logger = logging.getLogger(name)
    console_handler = logging.StreamHandler(sys.stdout)
    logger.setLevel(LOG_LEVEL)
    console_handler.setLevel(LOG_LEVEL)
    formatter = logging.Formatter(FORMAT)
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    return logger
