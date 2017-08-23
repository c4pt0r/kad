import logging
import sys
import logging.config

logger = logging.getLogger(__name__)

logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)

# create a logging format
formatter = logging.Formatter('%(asctime)s %(name)s [%(levelname)s] %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(handler)


debug = logger.debug
info = logger.info
warn = logger.warn
error = logger.error
