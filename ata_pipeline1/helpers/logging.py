import logging
import logging.config

# To use this logging config in another module:
# >>> from ata_pipeline0.helpers.logging import logging
# >>> logger = logging.getLogger(__name__)
# >>> logger.info("Hello, I'm a log")
# This adheres to the one-logger-per-module best practice
# (see: https://stackoverflow.com/questions/15727420/using-logging-in-multiple-modules)

# TODO: As soon as we have a top-level place to store all config variables, move this into it
# Details here: https://docs.python.org/3.9/library/logging.config.html#logging.config.dictConfig
CONFIG = {
    "version": 1,
    "disable_existing_loggers": True,  # Disables some annoying logs from boto3
    "formatters": {
        "standard": {
            # CloudWatch will already have timestamps for logs, but can add %(asctime)s.%(msecs).3d for timestamp if needed
            "format": "%(levelname)-8s  %(name)s:%(lineno)s: %(message)s",
            # "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "default": {"formatter": "standard", "class": "logging.StreamHandler"},
    },
    "root": {"handlers": ["default"], "level": "INFO"},
}

logging.config.dictConfig(CONFIG)
