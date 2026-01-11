import copy
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

    def _execute_command(self, context: CommandContext):
        extractor = context.cli.get_extractor(context.args.type)
        extractor.do_extract(context)


class DataExtractor:

    def do_extract(self, context: CommandContext):
        pass

    def _post_extract(self, data: Any, context: CommandContext):
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

        table_json = [row.to_json() for row in table.retrieve()]

        extract_file_path = Path(str(os.path.join(args.output, JSON.add_to(table_name))))
        extract_file_path.parent.mkdir(parents=True, exist_ok=True)

        formatted_data = copy.deepcopy(table_json)

        # Remove NULL values.
        for idx, record in enumerate(table_json):
            for column_name, column_value in record.items():
                if column_value is None:
                    del formatted_data[idx][column_name]

        content = {
            "metadata": {
                "table_name": table_name,
                "type": args.type,
                "extract_date": datetime.now().isoformat()
            },
            "data": self._post_extract(formatted_data, context)
        }

        if args.filter:
            content["metadata"]["filter"] = args.filter

        save_file(str(extract_file_path), content, as_json=True)

    def _post_extract(self, data: Any, context: CommandContext):
        """
        Allows to process retrieved table
        data and change data structure if
        needed.
        """
        return data
