import dataclasses
import sys
from typing import TYPE_CHECKING, Any

from kdb.manager import DatabaseManager
from kutil.logger import get_logger

if TYPE_CHECKING:
    from kamadbm.cli import DatabaseCLI


_logger = get_logger(__name__)


@dataclasses.dataclass
class CommandContext:
    args: Any
    database: DatabaseManager
    cli: "DatabaseCLI"


class CLICommand:

    def __init__(self, cli: "DatabaseCLI"):
        self.__cli = cli

    def execute(self, args):

        if args.database is None:
            _logger.error("Argument '--database' is required for import.")
            print("Argument '--database' is required for import.")
            sys.exit(1)

        context = CommandContext(
            args=args,
            database=DatabaseManager(args.database),
            cli=self.__cli
        )
        self._execute_command(context)

    def _execute_command(self, context: CommandContext):
        pass
