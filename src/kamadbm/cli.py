from importlib.metadata import entry_points
import argparse
import os
import sys
from typing import Final

from kutil.meta import SingletonMeta

from kamadbm.migrator import MigrateCommand
from kamadbm.extractor import DataExtractor, RegularExtractor, ExtractCommand
from kamadbm.importer import DataImporter, RegularImporter, ImportCommand


class DatabaseCLI(metaclass=SingletonMeta):

    Regular: Final[str] = "Regular"

    def __init__(self):
        self.__extractors: dict[str, DataExtractor] = {}
        self.__importers: dict[str, DataImporter] = {}
        self.__migration_paths: list[str] = []

        self.add_importer(RegularImporter())
        self.add_extractor(RegularExtractor())

    def add_migration_path(self, path: str):
        self.__migration_paths.append(path)

    @property
    def extra_migration_paths(self):
        return self.__migration_paths

    def get_importer(self, name: str):
        importer = self.__importers.get(name)
        return importer or self.__importers.get(self.Regular)

    def get_extractor(self, name: str):
        extractor = self.__extractors.get(name)
        return extractor or self.__extractors.get(self.Regular)

    def add_importer(self, importer: DataImporter):
        name = importer.__class__.__name__.replace("Importer", "")
        self.__importers[name] = importer

    def add_extractor(self, extractor: DataExtractor):
        name = extractor.__class__.__name__.replace("Extractor", "")
        self.__extractors[name] = extractor

    def run(self):

        """
        Command line tool
        that is used to perform database related operations.
        """

        self.__discover_plugins()

        parser = argparse.ArgumentParser(
            description="Database Management Service for SaveGem.",
            formatter_class=argparse.RawTextHelpFormatter
        )

        subparsers = parser.add_subparsers(
            title="Available Commands",
            dest="command",
            required=True,
            help="Select an operation to perform."
        )

        self.__add_migrate_command(subparsers)
        self.__add_import_command(subparsers)
        self.__add_extract_command(subparsers)

        if len(sys.argv) == 1:
            parser.print_help(sys.stderr)
            sys.exit(1)

        args = parser.parse_args()

        try:
            exit_code = args.func(args)
            sys.exit(exit_code)
        except Exception as e:
            print(f"\nCritical Error during execution: {e}", file=sys.stderr)
            sys.exit(1)

    def __add_migrate_command(self, subparsers):
        """
        Used to set up 'migrate' command.
        """

        migrate_command = MigrateCommand(self)
        migrate_parser = subparsers.add_parser(
            "migrate",
            help="Run Python-based database schema migrations."
        )

        migrate_parser.add_argument(
            "--migration_directories",
            required=True,
            nargs="+",
            type=str,
            help="Path to directory containing SQL migrations."
        )

        migrate_parser.add_argument(
            "--database",
            required=True,
            type=str,
            help="Path to SQLite file or name of in-memory database."
        )

        migrate_parser.set_defaults(func=migrate_command.execute)

    def __add_import_command(self, subparsers):
        """
        Used to set up 'import' command.
        """

        import_command = ImportCommand(self)
        import_parser = subparsers.add_parser(
            "import",
            help="Import table data from JSON definitions in source files."
        )

        import_parser.add_argument(
            "--database",
            required=True,
            type=str,
            help="Path to SQLite file or name of in-memory database."
        )

        import_parser.add_argument(
            "--file_name",
            type=str,
            help="Name of the import that needs to be imported."
        )

        import_parser.add_argument(
            "--definition_file",
            type=str,
            help="Name of the file containing names of table definitions that needs to be imported."
        )

        import_parser.set_defaults(func=import_command.execute)

    def __add_extract_command(self, subparsers):
        """
        Used to set up 'extract' command.
        """

        extract_command = ExtractCommand(self)
        extract_parser = subparsers.add_parser(
            "extract",
            help="Extract table data from database tables into JSON definitions."
        )

        extract_parser.add_argument(
            "--database",
            required=True,
            type=str,
            help="Path to SQLite file or name of in-memory database."
        )

        extract_parser.add_argument(
            "--table_name",
            required=True,
            type=str,
            help="Name of the table that should be extracted."
        )

        extract_parser.add_argument(
            "--type",
            default=self.Regular,
            choices=list(self.__extractors.keys()),
            type=str,
            help="Set type of data that is being extracted."
        )

        extract_parser.add_argument(
            "--filter",
            type=str,
            help="Set filter that would limit extracted dataset."
        )

        extract_parser.add_argument(
            '--output',
            default=os.path.join("output", "extract"),
            type=str,
            help='Output directory where extracted data would be placed.'
        )

        extract_parser.set_defaults(func=extract_command.execute)

    @staticmethod
    def __discover_plugins():
        plugins = entry_points(group="kama_dbm.plugins")

        for plugin in plugins:
            plugin.load()
