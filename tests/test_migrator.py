import datetime
import pytest

from pytest_mock import MockerFixture
from command import BaseCommandTest


class TestMigrateCommand(BaseCommandTest):

    @pytest.fixture
    def _command(self, _cli):
        from kamadbm.migrator import MigrateCommand
        return MigrateCommand(_cli)

    @pytest.fixture
    def _args(self, mocker: MockerFixture):
        args = mocker.MagicMock()
        args.migration_directories = ["/base/migrations"]

        return args

    @pytest.fixture
    def _cli(self, mocker: MockerFixture):
        cli = mocker.MagicMock()
        cli.extra_migration_paths = ["/extra/migrations"]

        return cli

    @pytest.fixture
    def migration_exists_mock(self, mocker):
        from kamadbm.migrator import MigrateCommand
        return mocker.patch.object(MigrateCommand, '_MigrateCommand__migration_exists')

    @pytest.fixture
    def update_schema_mock(self, mocker):
        from kamadbm.migrator import MigrateCommand
        return mocker.patch.object(MigrateCommand, '_MigrateCommand__update_schema_version')

    def test_execute_command_flow(self, mocker: MockerFixture, _command, _context):
        """
        Tests that the main entry point calls initialize and migrate.
        """

        from kamadbm.migrator import MigrateCommand

        mock_init = mocker.patch.object(MigrateCommand, '_MigrateCommand__initialize')
        mock_migrate = mocker.patch.object(MigrateCommand, '_MigrateCommand__migrate')

        _command._execute_command(_context)

        mock_init.assert_called_once_with(_context)
        mock_migrate.assert_called_once_with(_context)

    def test_initialize_creates_both_tables(self, _context):
        """
        Verifies that both tracking tables are initialized.
        """

        from kamadbm.migrator import MigrateCommand

        # Accessing private static method
        MigrateCommand._MigrateCommand__initialize(_context)  # noqa

        calls = [_call[0][0] for _call in _context.database.execute.call_args_list]

        assert any("CREATE TABLE IF NOT EXISTS schema_version" in sql for sql in calls)
        assert any("CREATE TABLE IF NOT EXISTS import_data_version" in sql for sql in calls)

    @pytest.mark.parametrize("has_record, expected", [
        (True, True),
        (False, False)
    ])
    def test_migration_exists(self, _context, has_record, expected):
        """
        Tests the database check for existing migration records.
        """

        from kamadbm.migrator import MigrateCommand

        _context.database.select().fetchone.return_value = (1,) if has_record else None
        _context.database.select.reset_mock()

        result = MigrateCommand._MigrateCommand__migration_exists(_context.database, "v1.sql")  # noqa

        assert result is expected
        _context.database.select.assert_called_once_with(
            "SELECT 1 FROM schema_version WHERE file_name = ?", ("v1.sql",)
        )

    def test_update_schema_version_fluent_api(self, _context, module_patch, db_table_mock, remove_extension_from_path_mock):
        """
        Tests the new fluent table API for updating schema versions.
        """

        from kamadbm.migrator import MigrateCommand

        # Mock datetime to ensure consistency
        mock_now = datetime.datetime(2026, 1, 22)
        module_patch("datetime.datetime").now.return_value = mock_now

        migration_name = "v2025_10_12_2205__Create_tables.sql"

        # The method now calls remove_extension_from_path internally
        remove_extension_from_path_mock.return_value = "v2025_10_12_2205__Create_tables"
        MigrateCommand._MigrateCommand__update_schema_version(_context.database, migration_name)  # noqa

        _context.database.retrieve_table.assert_called_with("schema_version")
        db_table_mock.add.assert_called_once_with(
            file_name="v2025_10_12_2205__Create_tables",
            version="2025.10.12.2205",
            description="Create tables",
            date_applied=mock_now,
            success=1
        )

    def test_migrate_skips_all_if_latest_exists(self, _context, migration_exists_mock, module_patch):
        """
        If the last migration in the list exists, the whole process should exit.
        """

        from kamadbm.migrator import MigrateCommand

        # Dir 1 and Dir 2
        module_patch("os.listdir", side_effect=[["x_last.sql"], ["a_first.sql"]])
        migration_exists_mock.return_value = True

        MigrateCommand._MigrateCommand__migrate(_context)  # noqa

        # Should have checked the last one (sorted alphabetically: x_last.sql)
        migration_exists_mock.assert_called_once()
        assert "x_last.sql" in migration_exists_mock.call_args[0][1]

        # Database script should NOT have run
        _context.database.connection().executescript.assert_not_called()

    def test_migrate_applies_only_missing_files(self, _context, migration_exists_mock, update_schema_mock,
                                                module_patch, read_file_mock):
        """
        Tests the full loop: discovering files across dirs and applying missing ones.
        """

        from kamadbm.migrator import MigrateCommand

        _context.args.migration_directories = ["/dir1"]
        _context.cli.extra_migration_paths = ["/dir2"]

        # One file per dir
        module_patch("os.listdir", side_effect=[["v1.sql"], ["v2.sql"]])
        read_file_mock.return_value = "SELECT 1;"

        # Scenario: v2 exists, but v1 is missing.
        # (Note: Logic checks latest first. If v2 exists, it returns.
        # To test the loop, latest must NOT exist.)
        migration_exists_mock.side_effect = [
            False,  # Check latest (v2.sql) -> No
            True,  # Loop check v1.sql -> Yes (skip)
            False  # Loop check v2.sql -> No (apply)
        ]

        MigrateCommand._MigrateCommand__migrate(_context)  # noqa

        # Verify executescript was only called for v2
        _context.database.connection().executescript.assert_called_once_with("SELECT 1;")
        update_schema_mock.assert_called_once_with(_context.database, "v2.sql")

    def test_update_schema_version_raises_on_invalid_name(self, _context, remove_extension_from_path_mock):
        """
        Tests that __update_schema_version raises RuntimeError when
        the migration filename format is invalid (missing '__').
        """

        from kamadbm.migrator import MigrateCommand

        # This name is missing the double underscore separator
        invalid_name = "v2026_01_22_invalid_format.sql"

        # We must mock remove_extension_from_path because it's called
        # before the validation check.
        remove_extension_from_path_mock.return_value = "v2026_01_22_invalid_format"

        with pytest.raises(RuntimeError) as error:
            MigrateCommand._MigrateCommand__update_schema_version(_context.database, invalid_name)  # noqa

        # Verify the error message matches the implementation
        assert "Migration v2026_01_22_invalid_format is invalid." in str(error.value)

        # Ensure the database was never touched after the error
        _context.database.retrieve_table.assert_not_called()