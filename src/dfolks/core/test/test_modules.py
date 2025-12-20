"""Test for utils."""

import importlib
import logging
import pkgutil
import sys

from dfolks.core.modules import import_all_submodules, set_logger


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


def test_set_logger_adds_file_handler(monkeypatch):
    created_files = []

    class FakeFileHandler(logging.Handler):
        def __init__(self, filename, mode="a"):
            super().__init__()
            created_files.append(filename)

    monkeypatch.setattr(logging, "FileHandler", FakeFileHandler)

    logger = set_logger(
        name="file_logger",
        level=logging.INFO,
        log_path="test.log",
    )

    # console + file handler
    assert len(logger.handlers) == 2
    assert "test.log" in created_files


def test_import_all_submodules(monkeypatch):
    imported = []

    # Mock importlib.import_module
    def fake_import_module(name):
        imported.append(name)

        # return a fake package object for the root package
        if name == "dfolks":

            class FakePackage:
                __path__ = ["fake/path"]
                __name__ = "dfolks"

            return FakePackage()

        return None

    monkeypatch.setattr(importlib, "import_module", fake_import_module)

    # Mock pkgutil.walk_packages
    def fake_walk_packages(path, prefix):
        assert path == ["fake/path"]
        assert prefix == "dfolks."
        return [
            (None, "dfolks.submodule1", False),
            (None, "dfolks.submodule2", False),
        ]

    monkeypatch.setattr(pkgutil, "walk_packages", fake_walk_packages)

    import_all_submodules(packages=("dfolks",))

    assert imported == [
        "dfolks",
        "dfolks.submodule1",
        "dfolks.submodule2",
    ]
