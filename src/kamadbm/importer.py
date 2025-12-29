import os
import sys
from pathlib import Path
from typing import Any

from kutil.file import read_file, file_checksum
from kutil.logger import get_logger

from kamadbm.command import CLICommand, CommandContext

_logger = get_logger(__name__)


class ImportCommand(CLICommand):

    def _execute_command(self, context: CommandContext):
        """
        Used to call database importer.
        If definition file was provided will
        read import all the files listed in it.

        If file name provided directly will import
        only that file.

        Will use import data metadata to decide
        which importer implementation should be invoked.
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
        Used to invoke importer for provided import data file.
        """

        import_file = read_file(file_path, as_json=True)
        import_type: str = import_file.get("metadata").get("type")
        importer = context.cli.get_importer(import_type)

        context.args.file_path = file_path
        importer.do_import(context)


class DataImporter:

    def do_import(self, context: CommandContext):
        pass

    def _format_data(self, data: Any, metadata: dict, context: CommandContext):
        pass


class RegularImporter(DataImporter):
    """
    Database importer.
    Allows to import data from
    JSON file to database table.

    Will remove all existing table data
    when importing.
    """

    def do_import(self, context: CommandContext):
        """
        Used to import data from JSON file
        into database.
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
            row_number = import_table.add_row()

            for column_name, column_value in record.items():
                import_table.set(row_number, column_name, column_value)

        import_table.save()

    def _format_data(self, data: Any, metadata: dict, context: CommandContext):
        """
        Allows to format JSON data before
        persisting it in database.

        Data should have a flat structure at the moment
        when it's being inserted into database. (list[dict])
        """
        return data
