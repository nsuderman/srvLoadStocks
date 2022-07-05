import logging
import logging.handlers as handlers
import os

location = os.getcwd()
name = location.split("\\")[-1]

log_level = logging.INFO
logger = logging.getLogger(name)
logger.setLevel(log_level)
formatter = logging.Formatter('%(asctime)s - %(levelname)-5s [%(filename)s:%(lineno)d] %(message)s',"%Y-%m-%d %H:%M:%S")
stream = logging.StreamHandler()
stream.setLevel(log_level)
stream.setFormatter(formatter)
logger.addHandler(stream)

logHandler = handlers.TimedRotatingFileHandler(location +f'\\logger\\logs\\{name}.log', when="midnight", interval=1)
logHandler.setLevel(log_level)
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)