import os
from datetime import datetime
from pathlib import Path
from typing import Any

from kutil.file import save_file
from kutil.file_type import JSON
from kutil.logger import get_logger
from kamadbm.command import CLICommand, CommandContext

_logger = get_logger(__name__)


class ExtractCommand(CLICommand):
    """
    CLI Command implementation for triggering the data extraction process.

    This class acts as the entry point for the 'extract' action, delegating
    the actual logic to the appropriate DataExtractor implementation.
    """

    def _execute_command(self, context: CommandContext):
        """
        Executes the extraction process based on the provided context.

        Args:
            context (CommandContext): The context containing CLI arguments and
                database connections.
        """

        extractor = context.cli.get_extractor(context.args.type)
        extractor.do_extract(context)


class DataExtractor:
    """
    Base class for all data extraction strategies.

    Defines the interface for extracting data from a source and performing
    post-extraction processing.
    """

    def do_extract(self, context: CommandContext):  # pragma: no cover
        """
        Performs the main extraction logic.

        Args:
            context (CommandContext): The context containing environment and
                execution parameters.
        """
        pass

    def _post_extract(self, data: Any, context: CommandContext):  # pragma: no cover
        """
        Optional hook to transform or process data after it has been retrieved.

        Args:
            data (Any): The raw data retrieved from the source.
            context (CommandContext): The execution context.

        Returns:
            Any: The processed data.
        """
        pass


class RegularExtractor(DataExtractor):
    """
    Database extractor.
    Used to extract table data and store it
    in JSON format.
    """

    def do_extract(self, context: CommandContext):
        """
        Used to extract data from table.

        Retrieves rows from the specified database table, applies optional
        filters, wraps the data in metadata, and saves the result as a
        JSON file.

        Args:
            context (CommandContext): The context containing database
                connections and CLI arguments like table_name, filter, and output.
        """

        args = context.args
        db = context.database

        _logger.info("Starting data extraction.")
        _logger.info("Extractor: %s", args.type)
        _logger.info("Table: %s", args.table_name)
        _logger.info("Output Directory: %s", args.output)

        table_name = args.table_name
        table = db.table(table_name)

        if args.filter:
            _logger.info("Filter: %s", args.filter)
            table.where(args.filter)

        table_data = [row.to_json(include_nulls=False) for row in table.retrieve()]
        extract_file_path = Path(str(os.path.join(args.output, JSON.add_extension(table_name))))
        extract_file_path.parent.mkdir(parents=True, exist_ok=True)

        content = {
            "metadata": {
                "table_name": table_name,
                "type": args.type,
                "extract_date": datetime.now().isoformat()
            },
            "data": self._post_extract(table_data, context)
        }

        if args.filter:
            content["metadata"]["filter"] = args.filter

        save_file(str(extract_file_path), content, as_json=True)

    def _post_extract(self, data: Any, context: CommandContext):
        """
        Allows to process retrieved table
        data and change data structure if
        needed.

        In this implementation, it returns the data unchanged.

        Args:
            data (Any): The list of JSON-serialized rows.
            context (CommandContext): The execution context.

        Returns:
            Any: The processed (unchanged) data.
        """
        return data
