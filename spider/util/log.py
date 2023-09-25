import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(pathname)s %(filename)s %(funcName)s %(lineno)s %(levelname)-8s %(message)s"

)

logger = logging.getLogger(__file__)
