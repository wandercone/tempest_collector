import logging

try:
    from colorlog import ColoredFormatter
    _has_colorlog = True
except ImportError:
    _has_colorlog = False

_FMT       = "%(log_color)s[%(asctime)s] [%(levelname)s] %(message)s"
_FMT_PLAIN = "[%(asctime)s] [%(levelname)s] %(message)s"
_DATEFMT   = "%Y-%m-%d %H:%M:%S"

_LOG_COLORS = {
    "DEBUG":    "cyan",
    "INFO":     "green",
    "WARNING":  "yellow",
    "ERROR":    "red",
    "CRITICAL": "bold_red",
}

def configure_logging(debug=False) -> logging.Logger:
    """Configure and return the tempest_collector logger."""
    logger = logging.getLogger("tempest_collector")

    if logger.handlers:
        return logger

    handler = logging.StreamHandler()

    if _has_colorlog:
        handler.setFormatter(ColoredFormatter(
            fmt=_FMT,
            datefmt=_DATEFMT,
            log_colors=_LOG_COLORS,
        ))
    else:
        handler.setFormatter(logging.Formatter(fmt=_FMT_PLAIN, datefmt=_DATEFMT))

    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    logger.propagate = False

    return logger
