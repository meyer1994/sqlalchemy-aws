import logging
import logging.config

CONFIG = {
    "version": 1,
    "formatters": {
        "detailed": {
            "format": "[%(asctime)s] [%(levelname)-8s] [%(name)s:%(lineno)d] - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "console": {
            "class": logging.StreamHandler,
            "level": logging.DEBUG,
            "formatter": "detailed",
        },
    },
    "loggers": {
        "sqla": {"level": logging.DEBUG, "handlers": ["console"]},
        # "sqlalchemy": {"level": "DEBUG", "handlers": ["console"]},
    },
}


def init():
    logging.config.dictConfig(CONFIG)
