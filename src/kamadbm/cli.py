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
    """
    The main Entry Point for the Kamadbm Command Line Interface.

    This class manages database migrations, data extraction, and data importation.
    It utilizes a Singleton pattern to ensure global access to CLI configurations
    and registered plugins.

    Attributes:
        Regular (Final[str]): The default identifier for standard extractors and importers.
    """

    Regular: Final[str] = "Regular"
    """
    Name of the regular extract type.
    """

    def __init__(self):
        """
        Initializes the DatabaseCLI with empty registries for extractors,
        importers, and migration paths.
        """

        self.__extractors: dict[str, DataExtractor] = {}
        self.__importers: dict[str, DataImporter] = {}
        self.__migration_paths: list[str] = []

    def post_init(self):
        """
        Populates the CLI with default 'Regular' implementations for
        importing and extracting data.
        """

        self.add_importer(RegularImporter())
        self.add_extractor(RegularExtractor())

    def add_migration_path(self, path: str):
        """
        Registers an additional file system path to search for migration scripts.

        Args:
            path (str): The directory path to add.
        """
        self.__migration_paths.append(path)

    @property
    def extra_migration_paths(self):
        """
        Returns a list of all manually registered migration paths.

        Returns:
            list[str]: A list of directory paths.
        """
        return self.__migration_paths

    def get_importer(self, name: str):
        """
        Retrieves a registered importer by name.
        Falls back to the 'Regular' importer if the specific name is not found.

        Args:
            name (str): The name of the importer to retrieve.

        Returns:
            DataImporter: The requested importer or the default 'Regular' importer.
        """

        importer = self.__importers.get(name)
        return importer or self.__importers.get(self.Regular)

    def get_extractor(self, name: str):
        """
        Retrieves a registered extractor by name.
        Falls back to the 'Regular' extractor if the specific name is not found.

        Args:
            name (str): The name of the extractor to retrieve.

        Returns:
            DataExtractor: The requested extractor or the default 'Regular' extractor.
        """

        extractor = self.__extractors.get(name)
        return extractor or self.__extractors.get(self.Regular)

    def add_importer(self, importer: DataImporter):
        """
        Registers a new DataImporter instance.
        The name is automatically derived from the class name (stripping 'Importer').

        Args:
            importer (DataImporter): The importer instance to register.
        """

        name = importer.__class__.__name__.replace("Importer", "")
        self.__importers[name] = importer

    def add_extractor(self, extractor: DataExtractor):
        """
        Registers a new DataExtractor instance.
        The name is automatically derived from the class name (stripping 'Extractor').

        Args:
            extractor (DataExtractor): The extractor instance to register.
        """

        name = extractor.__class__.__name__.replace("Extractor", "")
        self.__extractors[name] = extractor

    def run(self):
        """
        Main execution loop for the CLI tool.

        Handles plugin discovery, parses command-line arguments using argparse,
        and dispatches execution to the appropriate command handler.
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
        Configures the 'migrate' subcommand and its arguments.

        Args:
            subparsers: The argparse subparser object to attach to.
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
        Configures the 'import' subcommand and its arguments.

        Args:
            subparsers: The argparse subparser object to attach to.
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
        Configures the 'extract' subcommand and its arguments.

        Args:
            subparsers: The argparse subparser object to attach to.
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
        """
        Uses importlib.metadata to find and load external plugins registered
        under the 'kama_dbm.plugins' entry point group.
        """

        for plugin in entry_points(group="kama_dbm.plugins"):
            plugin.load()
