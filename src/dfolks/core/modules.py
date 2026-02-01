"""Modules for core classes.

1) set_logger
2) import_all_submodules

Need to work:
0) import all submodules: dynamic entrypoint.
"""

import importlib
import logging
import pkgutil
import sys

# Global logger
loggers = {}


def set_logger(name, level, log_path):
    """Set up a shared logger."""
    global loggers

    # if logger is already defined then use that logger.
    if loggers.get(name):
        return loggers.get(name)
    else:
        logger = logging.getLogger(name)
        logger.setLevel(level)  # Set the base logger level.

        # Define a formatter.
        formatter = logging.Formatter(
            fmt="%(asctime)s [Module: %(module)s-%(funcName)s] [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Create a console handler.
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)  # Set console-specific level.
        console_handler.setFormatter(formatter)  # Set console-specific fommatter.
        logger.addHandler(console_handler)

        # Create a file handler.
        if log_path:
            file_handler = logging.FileHandler(log_path, mode="a")
            file_handler.setLevel(level)  # Set file-specific level.
            file_handler.setFormatter(formatter)  # Set file-specific fommatter.
            logger.addHandler(file_handler)

        # Store logger in the global dict.
        loggers[name] = logger

        return logger


def import_all_submodules(packages=("dfolks",)):
    """Dynamically imports all submodules of a package."""
    for package_name in packages:
        package = importlib.import_module(package_name)
        for _, module_name, _ in pkgutil.walk_packages(
            package.__path__, package.__name__ + "."
        ):
            importlib.import_module(module_name)
