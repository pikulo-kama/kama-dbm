import os
import sys
from pathlib import Path
from typing import Any

from kutil.file import read_file, file_checksum
from kutil.logger import get_logger

from kamadbm.command import CLICommand, CommandContext

_logger = get_logger(__name__)


class ImportCommand(CLICommand):
    """
    Command implementation for importing data into the database.

    This command supports two modes of operation:
        1. Direct file import via the `file_path` argument.
        2. Batch import via a `definition_file` containing a list of relative file paths.
    """

    def _execute_command(self, context: CommandContext):
        """
        Orchestrates the import process based on the provided context arguments.

        If a definition file is used, it implements an idempotent check using SHA checksums
        stored in the `import_data_version` table to skip unchanged files.

        Args:
            context: The command execution context containing CLI arguments and
                the active database connection.
        """

        args = context.args
        db = context.database

        # Handle scenario where import file name provided directly.
        if hasattr(args, "file_path"):
            self.__invoke_importer_for_file(args.file_path, context)
            return

        definition_file_dir = Path(args.definition_file).parent
        definition_file: str = read_file(args.definition_file)
        files_to_import = []

        _logger.info("Importing definition file: %s", args.definition_file)

        # Handle definition_file argument.
        for line in definition_file.split("\n"):
            line = line.strip().replace("/", os.path.sep)

            # Allow comments and skip empty lines.
            if line.startswith("#") or len(line) == 0:
                continue

            file_path = os.path.join(definition_file_dir, line)
            actual_checksum = file_checksum(file_path)
            metadata = db.table("import_data_version") \
                .where("file_name = ?", line) \
                .retrieve()

            # Create entry if it doesn't exist.
            if metadata.is_empty:
                metadata.add(file_name=line).save()

            current_checksum = metadata.get_first("checksum")
            _logger.info("%s: current: %s, actual: %s", line, current_checksum, actual_checksum)

            # Only import data if checksum has changed.
            if current_checksum != actual_checksum:
                metadata.set_first("checksum", actual_checksum)
                files_to_import.append(file_path)
            else:
                _logger.info("Import file hasn't been changed. Skipping.")

            metadata.save()

        # Import files separately.
        for file_path in files_to_import:
            self.__invoke_importer_for_file(file_path, context)

    @classmethod
    def __invoke_importer_for_file(cls, file_path: str, context: CommandContext):
        """
        Reads the target file and delegates the import task to the appropriate DataImporter.

        Args:
            file_path: Absolute or relative path to the JSON data file.
            context: The command execution context.
        """

        import_file = read_file(file_path, as_json=True)
        import_type: str = import_file.get("metadata").get("type")
        importer = context.cli.get_importer(import_type)

        context.args.file_path = file_path
        importer.do_import(context)


class DataImporter:
    """
    Base class for all database importer implementations.
    """

    def do_import(self, context: CommandContext):  # pragma: no cover
        """
        Execute the import logic. Must be overridden by subclasses.
        """
        pass

    def _format_data(self, data: Any, metadata: dict, context: CommandContext):  # pragma: no cover
        """
        Apply transformations to raw data before database insertion.
        """
        pass


class RegularImporter(DataImporter):
    """
    Standard database importer for flat JSON data.

    This importer maps JSON list entries directly to database table rows.
    Note: This implementation performs a destructive import (truncates target table
    or filtered subset) before inserting new data.
    """

    def do_import(self, context: CommandContext):
        """
        Imports data from a JSON file into the specified database table.

        The method expects a JSON structure containing 'metadata' (table_name, type)
        and 'data' (list of dictionaries).

        Args:
            context: The command execution context.

        Raises:
            SystemExit: If `file_path` is missing from the context arguments.
        """

        args = context.args
        db = context.database

        if args.file_path is None:
            _logger.error("Argument '--file_path' is required for import.")
            print("Argument '--file_path' is required for import.")
            sys.exit(1)

        import_file = read_file(args.file_path, as_json=True)
        metadata = import_file.get("metadata", {})
        table_name = metadata.get("table_name")
        filter_string = metadata.get("filter")
        data: list[dict] = import_file.get("data", [])
        data = self._format_data(data, metadata, context)

        _logger.info("Importing %s.", args.file_path)
        _logger.info("Importer: %s", metadata.get("type"))
        _logger.info("Table: %s", table_name)

        import_table = db.table(table_name)

        if filter_string:
            _logger.info("Filter: %s", filter_string)
            import_table.where(filter_string)

        _logger.info("-----------------")

        import_table.retrieve()

        # Remove all existing data.
        import_table.remove_all()
        import_table.save()

        for record in data:
            import_table.add(**record)

        import_table.save()

    def _format_data(self, data: Any, metadata: dict, context: CommandContext) -> list[dict]:
        """
        Ensures data is in a flat list-of-dicts format for database persistence.

        Args:
            data: The raw data extracted from JSON.
            metadata: Metadata associated with the import.
            context: The execution context.

        Returns:
            The processed list of dictionaries ready for insertion.
        """
        return data
