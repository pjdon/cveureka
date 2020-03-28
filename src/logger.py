from typing import Optional, Union
import logging
from pathlib import Path


def empty_logger():
    """Returns a logger that does nothing"""
    return logging.getLogger()


class ContextAdapter(logging.LoggerAdapter):

    def process(self, msg, kwargs):
        context = self.extra.get('context', None)
        return (
            f"[{self.extra['context']}] {msg}" if context else msg,
            kwargs
        )

# custom type for either a Logger of an adapted Logger that behaves like one
ContextLoggable = Union[ContextAdapter, logging.Logger]


class LoggerHub:
    def __init__(
            self,
            name: Optional[str] = None,
            file_path: Optional[str] = None,
            file_level=1,
            file_mode='a',
            file_encoding='utf-8',
            stream_level=logging.INFO,
            file_fmt="%(asctime)s %(levelname)s:%(funcName)s %(message)s",
            stream_fmt="%(asctime)s %(message)s",
            datefmt="%H:%M:%S"
    ):

        if name:
            self._empty = False

            self.logger = logging.getLogger(name)
            self.logger.setLevel(file_level)
            if file_path:
                # create directory if it doesn't exist
                Path(file_path).parents[0].mkdir(parents=True, exist_ok=True)
                self.file_handler = logging.FileHandler(
                    file_path, file_mode, file_encoding
                )
                self.file_handler.setLevel(file_level)
                self.file_formatter = logging.Formatter(file_fmt, datefmt)
                self.file_handler.setFormatter(self.file_formatter)
                self.logger.addHandler(self.file_handler)

            self.stream_formatter = logging.Formatter(stream_fmt, datefmt)
            self.stream_handler = logging.StreamHandler()
            self.stream_handler.setLevel(stream_level)
            self.stream_handler.setFormatter(self.stream_formatter)
            self.logger.addHandler(self.stream_handler)
        else:
            self._empty = True
            self.logger = empty_logger()

    def context(
            self, context_name: Optional[str] = None
    ) -> ContextLoggable:
        if self._empty is False:
            return ContextAdapter(
                self.logger, dict(context=context_name)
            )
        else:
            return self.logger


def empty_logger_hub():
    """Returns a logger hub with an empty logger"""
    return LoggerHub()
