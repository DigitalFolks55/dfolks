"""Test for variablized class."""

import logging
import sys

from dfolks.core.utils import set_logger


def test_set_logger():
    """Test for setting up a logger."""
    logger = set_logger(name="test_logger", level="DEBUG", log_path=None)
    assert logger.name == "test_logger"
    assert logger.level == 10  # DEBUG level is 10
    assert len(logger.handlers) == 1  # Only console handler should be present
    assert isinstance(logger.handlers[0], logging.StreamHandler)
    assert logger.handlers[0].level == 10  # Console handler should also be DEBUG level
    assert (
        logger.handlers[0].formatter._fmt
        == "%(asctime)s [Module: %(module)s-%(funcName)s] [%(levelname)s] %(message)s"
    )
    assert logger.handlers[0].formatter.datefmt == "%Y-%m-%d %H:%M:%S"
    assert (
        logger.handlers[0].stream == sys.stdout
    )  # Console handler should log to stdout
