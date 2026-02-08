import logging.config

_DEFAULT_LOGGING_SETTINGS = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
        },
    },
    "formatters": {
        "standard": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "loggers": {
        # Root logger catch-all
        "": {
            "handlers": ["console"],
            "level": "INFO",
            "propagate": True,
        },
        "verda_cloud_provider": {
            "handlers": ["console"],
            "level": "INFO",
            "propagate": False,
        },
    },
}


def setup_logging(log_level: str) -> None:
    """
    Setup logging configuration.

    Args:
        log_level: String level like "DEBUG", "INFO", etc.
    """
    config = _DEFAULT_LOGGING_SETTINGS.copy()

    # Update the level for your specific package
    if "verda_cloud_provider" in config["loggers"]:
        config["loggers"]["verda_cloud_provider"]["level"] = log_level.upper()

    logging.config.dictConfig(config)
