from logging import StreamHandler, Formatter, DEBUG, INFO
import logging


def getLogger(name):
    # log settings
    log = logging.getLogger(name)
    handler = StreamHandler()
    handler.setLevel(DEBUG)
    handler.setFormatter(Formatter('%(asctime)s- %(name)s - %(levelname)s - %(message)s'))
    log.setLevel(DEBUG)
    log.addHandler(handler)

    return log
