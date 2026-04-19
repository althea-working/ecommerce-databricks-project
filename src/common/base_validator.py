from typing import List, Tuple
from src.common.logger import get_logger
from src.common.notifier import SlackNotifier

logger = get_logger(__name__)


class BaseValidator:

    def __init__(self, spark, table_name: str, webhook_url: str = None):
        self.spark = spark
        self.table_name = table_name
        self.errors: List[str] = []
        self.notifier = SlackNotifier(webhook_url) if webhook_url else None

    def run(self):
        logger.info(f"Start validation for {self.table_name}")

        self.validate()

        if self.errors:
            msg = self._format_errors()
            logger.error(msg)

            if self.notifier:
                self.notifier.send(msg)

            raise Exception(msg)
        else:
            logger.info(f"Validation passed for {self.table_name}")

    def validate(self):
        """
        Override in subclass
        """
        raise NotImplementedError

    def add_error(self, message: str):
        self.errors.append(message)

    def _format_errors(self) -> str:
        error_lines = "\n".join([f"• {e}" for e in self.errors])

        return (
            f":x: *Data Validation Failed*\n"
            f"> *Table:* `{self.table_name}`\n"
            f"> *Error Count:* {len(self.errors)}\n\n"
            f"*Details:*\n{error_lines}"
        )

