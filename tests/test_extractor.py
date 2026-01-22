import datetime

import pytest
from pytest_mock import MockerFixture

from command import BaseCommandTest


class TestExtractCommand(BaseCommandTest):

    def test_extract_command_execute(self, _context):

        from kamadbm.extractor import ExtractCommand

        extractor = _context.cli.get_extractor.return_value

        command = ExtractCommand(_context.cli)
        command._execute_command(_context)

        _context.cli.get_extractor.assert_called_once_with(_context.args.type)
        extractor.do_extract.assert_called_once_with(_context)


class TestRegularExtractor:

    @pytest.fixture(autouse=True)
    def _setup(self, module_patch, _datetime_now):
        """
        Mocks file system operations and datetime for consistency.
        """

        module_patch("os.path.join", return_value="/mock/root/test_output_dir/test_table.json")
        datetime_mock = module_patch("datetime")
        datetime_mock.now.return_value = _datetime_now

    @pytest.fixture
    def path_mock(self, module_patch):
        return module_patch("Path")

    @pytest.fixture
    def _custom_extractor_mock(self):
        from kamadbm.extractor import RegularExtractor

        class MockCustomExtractor(RegularExtractor):
            """
            A mock custom extractor used for testing dynamic loading.
            """

            def do_extract(self, args):
                # Simply record that this was called
                self.extracted_data = True

            def __init__(self):
                super().__init__()
                self.extracted_data = False

        return MockCustomExtractor

    @pytest.fixture
    def _cli(self, mocker: MockerFixture):
        return mocker.MagicMock()

    @pytest.fixture
    def _context(self, mocker: MockerFixture, db_manager_mock, _cli):
        """
        Fixture to provide a mock argparse Namespace object.
        """

        from kamadbm.cli import DatabaseCLI
        from kamadbm.command import CommandContext

        args = mocker.MagicMock()
        args.type = DatabaseCLI.Regular
        args.table_name = "test_table"
        args.output = "test_output_dir"
        args.filter = None

        return CommandContext(
            args=args,
            database=db_manager_mock.return_value,
            cli=_cli
        )

    @pytest.fixture
    def _datetime_now(self):
        return datetime.datetime(2025, 11, 27, 10, 0, 0)

    def test_regular_extractor_calls_db_and_saves_file(self, _context, save_file_mock, _datetime_now, db_manager_mock,
                                                       db_table_mock, path_mock):
        """
        Tests the main success path of do_extract without filtering.
        """

        from kamadbm.extractor import RegularExtractor
        from kdb.table import DatabaseRow

        db_table_mock.retrieve.return_value = [
            DatabaseRow("TEST_TABLE", 1, (1, "Alice", None), ["id", "name", "value"]),
            DatabaseRow("TEST_TABLE", 1, (2, "Bob", 42), ["id", "name", "value"]),
        ]

        extractor = RegularExtractor()
        extractor.do_extract(_context)

        # 1. DB interaction check
        db_manager_mock.return_value.table.assert_called_once_with("test_table")
        db_table_mock.retrieve.assert_called_once()
        db_table_mock.where.assert_not_called()

        # 2. File system checks
        path_mock.return_value.parent.mkdir.assert_called_once_with(parents=True, exist_ok=True)

        expected_content = {
            "metadata": {
                "table_name": "test_table",
                "type": "Regular",
                "extract_date": _datetime_now.isoformat()
            },
            "data": [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob", "value": 42}
            ]
        }

        save_file_mock.assert_called_once_with(
            str(path_mock.return_value),
            expected_content,
            as_json=True
        )

    def test_regular_extractor_applies_filter(self, _context, save_file_mock):
        """
        Tests that a filter argument correctly invokes table.where() and adds filter to metadata.
        """

        from kamadbm.extractor import RegularExtractor

        _context.args.filter = "column > 10"

        extractor = RegularExtractor()
        extractor.do_extract(_context)

        # 2. Metadata check (requires inspecting the save_file call)
        saved_content = save_file_mock.call_args[0][1]
        assert saved_content["metadata"]["filter"] == "column > 10"

    def test_regular_extractor_calls_post_extract(self, mocker, _context, save_file_mock, db_table_mock):
        """
        Tests that the _post_extract method is correctly called with processed data.
        """

        from kamadbm.extractor import RegularExtractor
        from kdb.table import DatabaseRow

        # Mock _post_extract to change the data structure
        mock_post_extract = mocker.patch.object(RegularExtractor, '_post_extract', return_value=["Modified Data"])
        db_table_mock.retrieve.return_value = [
            DatabaseRow("TEST_TABLE", 1, (1, "Alice", None), ["id", "name", "value"]),
            DatabaseRow("TEST_TABLE", 1, (2, "Bob", 42), ["id", "name", "value"]),
        ]

        extractor = RegularExtractor()
        extractor.do_extract(_context)

        expected_input_to_post_extract = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob", "value": 42}
        ]
        mock_post_extract.assert_called_once()
        # Need to check the content of the argument, which is a deep copy
        assert mock_post_extract.call_args[0][0] == expected_input_to_post_extract

        # 2. Check the saved file content used the return value from _post_extract
        saved_content = save_file_mock.call_args[0][1]
        assert saved_content["data"] == ["Modified Data"]
