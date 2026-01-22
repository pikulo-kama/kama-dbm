import dataclasses
import sys
import os
from typing import TYPE_CHECKING, Any

from kdb.manager import DatabaseManager
from kutil.logger import get_logger

if TYPE_CHECKING:
    from kamadbm.cli import DatabaseCLI


_logger = get_logger(__name__)


@dataclasses.dataclass
class CommandContext:
    """
    Data container that holds the necessary state for executing a CLI command.

    Attributes:
        args (Any): The parsed command-line arguments from argparse.
        database (DatabaseManager): An initialized instance of the database manager.
        cli (DatabaseCLI): A reference to the main CLI application instance.
    """

    args: Any
    database: DatabaseManager
    cli: "DatabaseCLI"


class CLICommand:
    """
    Base class for all CLI command implementations.

    This class provides the boilerplate for setting up the execution context,
    including database initialization and argument validation.
    """

    def __init__(self, cli: "DatabaseCLI"):
        """
        Initializes the command with a reference to the main CLI.

        Args:
            cli (DatabaseCLI): The parent CLI instance.
        """
        self.__cli = cli

    def execute(self, args):
        """
        The entry point called by argparse. Performs initial validation and
        prepares the CommandContext before calling the internal execution logic.

        Args:
            args (Any): The arguments parsed from the command line.

        Raises:
            SystemExit: If the required '--database' argument is missing.
        """

        if args.database is None:
            _logger.error("Argument '--database' is required for import.")
            print("Argument '--database' is required for import.")
            sys.exit(1)

        context = CommandContext(
            args=args,
            database=DatabaseManager(os.path.expandvars(args.database)),
            cli=self.__cli
        )
        self._execute_command(context)

    def _execute_command(self, context: CommandContext):  # pragma: no cover
        """
        Abstract method meant to be overridden by subclasses to implement
        specific command logic.

        Args:
            context (CommandContext): The prepared execution context.
        """
        pass
