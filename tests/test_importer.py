from unittest.mock import call
from pytest_mock import MockerFixture
import pytest

from command import BaseCommandTest


class TestImportCommand(BaseCommandTest):
    """
    Groups all unit tests for the ImportCommand class.
    """

    @pytest.fixture
    def _command(self, _cli):
        from kamadbm.importer import ImportCommand
        return ImportCommand(_cli)

    def test_execute_direct_file_path(self, read_file_mock, _command, _context):
        """
        Verifies that providing file_path bypasses definition file logic.
        """

        _context.args.file_path = "/data/single_file.json"
        _context.args.definition_file = "/path/to/definition"

        read_file_mock.return_value = {"metadata": {"type": "json_type"}}
        importer_mock = _context.cli.get_importer.return_value

        _command._execute_command(_context)

        importer_mock.do_import.assert_called_once()
        assert call(_context.args.definition_file) not in read_file_mock.call_args_list

    @pytest.mark.parametrize("checksum_match, expected_import_count", [
        (True, 0),  # Checksum same: Skip
        (False, 1)  # Checksum different: Import
    ])
    def test_execute_definition_file_logic(self, checksum_match, expected_import_count, _context, _command,
                                           read_file_mock, file_checksum_mock, module_patch, mocker: MockerFixture):
        """
        Tests the logic for skipping or importing files based on checksums.
        """

        # Setup context
        if hasattr(_context.args, "file_path"):
            delattr(_context.args, "file_path")

        _context.args.definition_file = "dir/manifest.txt"
        module_patch("os.path.sep", "/")

        # Mocks
        actual_hash = "abc-123"
        current_hash = "abc-123" if checksum_match else "old-hash"

        # Mock reading manifest then reading the data file
        read_file_mock.side_effect = ["file1.json", {"metadata": {"type": "type_a"}}]
        file_checksum_mock.return_value = actual_hash

        # Mock DB metadata record
        mock_metadata = mocker.MagicMock(is_empty=True)
        mock_metadata.get_first.return_value = current_hash
        _context.database.table().where().retrieve.return_value = mock_metadata

        importer_mock = _context.cli.get_importer.return_value

        _command._execute_command(_context)

        mock_metadata.add.assert_called()
        assert importer_mock.do_import.call_count == expected_import_count

    def test_handle_empty_lines_and_comments(self, _context, _command, read_file_mock):
        """
        Verifies the parser skips comments and empty lines in the definition file.
        """

        _context.args.definition_file = "manifest.txt"

        if hasattr(_context.args, "file_path"):
            delattr(_context.args, "file_path")

        # File with comments and empty lines
        read_file_mock.return_value = "# Header\n\n  \n# Another comment"

        _command._execute_command(_context)

        # Check that database wasn't even queried because no valid files were found
        _context.database.table.assert_not_called()


class TestRegularImporter:

    @pytest.fixture
    def _custom_importer_mock(self):
        from kamadbm.importer import RegularImporter

        class MockCustomImporter(RegularImporter):
            """
            A mock custom importer used for testing dynamic loading.
            """

            def do_import(self, args):
                self.imported_data = True

            def __init__(self):
                super().__init__()
                self.imported_data = False

        return MockCustomImporter

    @pytest.fixture
    def _cli(self, mocker: MockerFixture):
        return mocker.MagicMock()

    @pytest.fixture
    def _context(self, mocker: MockerFixture, db_manager_mock, _cli):
        """
        Fixture to provide a mock argparse Namespace object.
        """

        from kamadbm.command import CommandContext

        args = mocker.MagicMock()
        args.file_path = "/path/to/file"

        return CommandContext(
            args=args,
            database=db_manager_mock.return_value,
            cli=_cli
        )

    @pytest.fixture
    def _custom_importers(self, get_members_mock, _custom_importer_mock):

        from kamadbm.importer import RegularImporter

        importers = [
            ("RegularImporter", RegularImporter),
            ("CustomImporter", _custom_importer_mock)
        ]

        get_members_mock.return_value = importers
        return dict(importers)

    def test_regular_importer_exits_on_missing_file_name(self, _context, db_manager_mock, read_file_mock):
        """
        Tests the critical exit path when file_name is missing.
        """

        from kamadbm.importer import RegularImporter

        _context.args.file_path = None

        importer = RegularImporter()

        with pytest.raises(SystemExit):
            importer.do_import(_context)

        db_manager_mock.assert_not_called()
        read_file_mock.assert_not_called()

    def test_regular_importer_performs_truncate_and_insert(self, _context, read_file_mock, db_table_mock,
                                                           db_manager_mock):
        """
        Tests the successful data import, checking for remove_all and add_row/set.
        """

        from kamadbm.importer import RegularImporter

        import_data = [
            {"id": 1, "name": "A"},
            {"id": 2, "name": "B"}
        ]

        # Mock the input file
        read_file_mock.return_value = {
            "metadata": {"type": "Regular", "table_name": "users", "filter": None},
            "data": import_data
        }

        # Configure add_row to return row numbers
        db_table_mock.add_row.side_effect = [1, 2]

        importer = RegularImporter()
        importer.do_import(_context)

        # 1. Table selection and filter check
        db_manager_mock.return_value.table.assert_called_once_with("users")
        db_table_mock.where.assert_not_called()

        # 2. Deletion/Truncate check
        db_table_mock.retrieve.assert_called_once()
        db_table_mock.remove_all.assert_called_once()

        db_table_mock.add.assert_has_calls([
            call(id=1, name="A"),
            call(id=2, name="B"),
        ])
        assert db_table_mock.add.call_count == 2

        # 4. Save calls (one after remove_all, one after insertion)
        assert db_table_mock.save.call_count == 2

    def test_regular_importer_applies_filter_before_remove_all(self, _context, read_file_mock, db_table_mock):
        """
        Tests that a filter is applied to the table object before remove_all.
        """

        from kamadbm.importer import RegularImporter

        # Mock the input file with a filter
        read_file_mock.return_value = {
            "metadata": {"type": "Regular", "table_name": "users", "filter": "is_active = 0"},
            "data": [{"id": 1, "name": "A"}]
        }

        importer = RegularImporter()
        importer.do_import(_context)

        # The filter should be applied via where()
        db_table_mock.where.assert_called_once_with("is_active = 0")

        # All subsequent operations (retrieve, remove_all, save) should operate on the filtered dataset.
        db_table_mock.retrieve.assert_called_once()
        db_table_mock.remove_all.assert_called_once()

    def test_regular_importer_calls_format_data(self, mocker: MockerFixture, _context, read_file_mock, db_table_mock):
        """
        Tests that _format_data is called correctly.
        """

        from kamadbm.importer import RegularImporter

        raw_data = [{"id": 1, "name": "A"}]
        formatted_data = [{"id": 1, "name": "A", "status": "processed"}]

        read_file_mock.return_value = {
            "metadata": {"type": "Regular", "table_name": "users"},
            "data": raw_data
        }

        # Mock _format_data to return processed data
        mock_format = mocker.patch.object(RegularImporter, '_format_data', return_value=formatted_data)

        db_table_mock.add_row.return_value = 1

        importer = RegularImporter()
        importer.do_import(_context)

        # Check that _format_data was called with raw data
        mock_format.assert_called_once_with(raw_data, {"type": "Regular", "table_name": "users"}, _context)

        # Check that the insertion used the formatted data
        db_table_mock.add.assert_called_once_with(
            id=1,
            name="A",
            status="processed"
        )
