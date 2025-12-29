import datetime
import os
from pathlib import Path

from kdb.manager import DatabaseManager
from kutil.file import read_file, remove_extension_from_path
from kutil.logger import get_logger

from kamadbm.command import CLICommand, CommandContext

_logger = get_logger(__name__)


class MigrateCommand(CLICommand):

    def _execute_command(self, context: CommandContext):
        _logger.info("Starting database upgrade.")
        self.__initialize(context)
        self.__migrate(context)

    @staticmethod
    def __initialize(context: CommandContext):
        """
        Used to create schema version and import
        data version tables.

        Tables are used to keep track of what
        migrations have been already executed and what data has been
        imported.
        """

        context.database.execute("""
            CREATE TABLE IF NOT EXISTS schema_version (
                id INTEGER PRIMARY KEY,
                file_name VARCHAR,
                version VARCHAR,
                description VARCHAR,
                date_applied VARCHAR,
                success INTEGER
            )
        """)

        context.database.execute("""            
            CREATE TABLE IF NOT EXISTS import_data_version (
                file_name   VARCHAR PRIMARY KEY,
                checksum    VARCHAR
            )
        """)

    @classmethod
    def __migrate(cls, context: CommandContext):
        """
        Used to execute all migrations that haven't
        been executed yet.

        Will also update schema_version table for
        all new migrations that have been executed.
        """

        db = context.database
        migration_directories = context.cli.extra_migration_paths + context.args.migration_directories
        migrations = []

        for directory in migration_directories:
            for migration_file_name in os.listdir(directory):
                migrations.append(os.path.join(directory, migration_file_name))

        migrations.sort(key=os.path.basename)
        last_migration_name = migrations[-1]

        _logger.info("Latest observed migration: %s.", last_migration_name)
        if cls.__migration_exists(db, last_migration_name):
            _logger.info("No migrations to perform. Exiting.")
            return

        for file_path in migrations:
            file_name = Path(file_path).name

            if cls.__migration_exists(db, file_name):
                _logger.info("Migration %s has already been executed. Skipping.", file_name)
                continue

            _logger.info("Applying migration %s.", file_name)
            script = read_file(file_path)
            db.connection().executescript(script)

            cls.__update_schema_version(db, file_name)

        _logger.info("All migrations have been executed.")

    @classmethod
    def __migration_exists(cls, manager: DatabaseManager, migration_name: str):
        """
        Used to check whether migration with provided name already exists.
        """

        cursor = manager.select("SELECT 1 FROM schema_version WHERE file_name = ?", (migration_name,))
        return cursor.fetchone() is not None

    @classmethod
    def __update_schema_version(cls, manager: DatabaseManager, migration_name: str):
        """
        Used to add migration to schema_version table.
        """

        migration_name = remove_extension_from_path(migration_name)
        parts = migration_name.split("__")

        if len(parts) != 2:
            raise RuntimeError(f"Migration {migration_name} is invalid.")

        version, description = parts
        version = version.replace("v", "").replace("_", ".")
        description = description.replace("_", " ")

        manager.retrieve_table("schema_version").add(
            file_name=migration_name,
            version=version,
            description=description,
            date_applied=datetime.datetime.now(),
            success=1
        ).save()
