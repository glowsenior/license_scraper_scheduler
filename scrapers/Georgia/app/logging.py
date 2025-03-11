import logging
import time
from typing import Optional
from rich.console import Console
from rich.logging import RichHandler
from .singleton import SingletonMeta


class Logger:
    """
    A class that provides a logger with a console handler.
    """

    _logger = None

    def __init__(self):
        """
        Initialize the logger with a console handler.
        """
        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(logging.INFO)

    def info(self, detail: Optional[str] = None, name="Info", span=None, **kwargs):
        """
        Log an info message.
        """
        # if len(detail) > 0:
        if detail and not span:
            self._logger.info(detail)

    def warning(self, detail: Optional[str] = None):
        """
        Log a warning message.
        """
        self._logger.warning(detail)

    def error(self, detail: Optional[str] = None, name="Error", span=None, **kwargs):
        """
        Log an error message.
        """
        self._logger.error(detail)


class AppLogger(metaclass=SingletonMeta):
    """
    A singleton class that provides a Logger instance.
    """

    _logger = None

    def __init__(self):
        """
        Initialize the AppLogger with a Logger instance.
        """
        self._logger = Logger()

    def get_logger(self):
        """
        Get the Logger instance.
        """
        return self._logger


class RichConsoleHandler(RichHandler):
    """
    A custom RichHandler that uses a Console instance with a width of 300 and a style.
    """

    def __init__(self, width=300, style=None, **kwargs):
        """
        Initialize the RichConsoleHandler with a Console instance.
        """
        super().__init__(
            console=Console(color_system="256", width=width, style=style), **kwargs
        )


class ElapsedTimeLogger:
    """
    Context manager to log elapsed time for code execution.

    Examples:
        Use `ElapsedTimeLogger` to measure and log the time taken by a block of code.

        ```python
        with ElapsedTimeLogger("Example task"):
            # Code block to measure time for
            time.sleep(2)
        ```

    Attributes:
        message (str): The message to log at the start and end of the context.
    """

    _logger = AppLogger().get_logger()

    def __init__(self, message):
        """
        Initialize the ElapsedTimeLogger with a message.
        """
        self.message = message

    def __enter__(self):
        """
        Enter the context manager.
        """
        self._logger.info(self.message)
        self.start = time.time()

    def __exit__(self, *args):
        """
        Exit the context manager.
        """
        elapsed_time = time.time() - self.start
        self._logger.info(f"Finished {self.message} in {elapsed_time} seconds")