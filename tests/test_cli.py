import pytest
from pytest_mock import MockerFixture


class TestDatabaseCLI:

    # Mock dependencies to prevent real code execution
    @pytest.fixture(autouse=True)
    def _setup(self, sys_exit_mock):
        pass

    @pytest.fixture
    def sys_exit_mock(self, module_patch):
        return module_patch("sys.exit")

    @pytest.fixture
    def migrate_command_mock(self, module_patch):
        return module_patch("MigrateCommand").return_value

    @pytest.fixture
    def import_command_mock(self, module_patch):
        return module_patch("ImportCommand").return_value

    @pytest.fixture
    def extract_command_mock(self, module_patch):
        return module_patch("ExtractCommand").return_value

    @pytest.fixture
    def _parser_mock(self, module_patch):
        return module_patch('argparse.ArgumentParser')

    @pytest.fixture
    def _cli(self):
        from kamadbm.cli import DatabaseCLI
        return DatabaseCLI()

    @pytest.fixture
    def run_main_with_args(self, mocker: MockerFixture, _cli):
        """
        Utility to run main() with mocked sys.argv.
        """

        def run(args):
            mocker.patch('sys.argv', ['cli_manager.py'] + args)
            _cli.run()

        return run

    def test_migrate_command_dispatch(self, migrate_command_mock, run_main_with_args):
        """
        Verify 'migrate' command calls DatabaseInitializer.run.
        """

        run_main_with_args(['migrate'])

        # Check that the assigned function was called
        migrate_command_mock.execute.assert_called_once()

        # The actual function receives the parsed args object as its first argument
        called_args = migrate_command_mock.execute.call_args[0][0]
        assert called_args.command == 'migrate'


    def test_import_command_dispatch(self, import_command_mock, run_main_with_args):
        """
        Verify 'import' command calls invoke_importer with arguments.
        """

        args = ['import', '--file_name', 'users', '--definition_file', 'test.def']
        run_main_with_args(args)

        # Check that the assigned function was called
        import_command_mock.execute.assert_called_once()

        # Check that the parsed args were passed to the importer
        called_args = import_command_mock.execute.call_args[0][0]
        assert called_args.command == 'import'
        assert called_args.file_name == 'users'
        assert called_args.definition_file == 'test.def'


    def test_extract_command_dispatch_and_defaults(self, run_main_with_args, extract_command_mock):
        """
        Verify 'extract' command calls invoke_extractor with correct defaults.
        """

        from kamadbm.cli import DatabaseCLI
        run_main_with_args([
            'extract', '--table_name', 'config_data',
            "--database", "/path/to/db"
        ])

        extract_command_mock.execute.assert_called_once()

        # Check defaults and required arguments
        called_args = extract_command_mock.execute.call_args[0][0]
        assert called_args.table_name == 'config_data'
        assert called_args.database == '/path/to/db'
        assert called_args.type == DatabaseCLI.Regular  # Checks the default value
        assert called_args.output.startswith('output')  # Checks the default path


    def test_extract_command_choices_are_correctly_built(self, _parser_mock, _cli, run_main_with_args):
        """
        Verify the 'type' choices include 'regular' plus discovered extractors.
        """

        from kamadbm.cli import DatabaseCLI
        from kamadbm.extractor import DataExtractor

        class TableExtractor(DataExtractor): pass
        class TreeExtractor(DataExtractor): pass

        subparsers_mock = _parser_mock.return_value.add_subparsers.return_value
        add_argument_mock = subparsers_mock.add_parser.return_value.add_argument

        _cli.add_extractor(TableExtractor())
        _cli.add_extractor(TreeExtractor())

        run_main_with_args([
            'extract', '--table_name', 'config_data',
            "--database", "/path/to/db"
        ])

        # Find the call that configured the '--type' argument
        type_arg_call = None
        for call_obj in add_argument_mock.call_args_list:
            if call_obj.args[0] == '--type':
                type_arg_call = call_obj
                break

        assert type_arg_call is not None
        assert type_arg_call.kwargs['choices'] == [DatabaseCLI.Regular, "Table", "Tree"]


    def test_main_exits_on_no_args(self, run_main_with_args, sys_exit_mock, _parser_mock):
        """
        Verify main() exits and prints help if no arguments are provided.
        """

        run_main_with_args([])

        # The parser's print_help method should have been called
        _parser_mock.return_value.print_help.assert_called_once()
        sys_exit_mock.assert_called()


    def test_main_handles_exception_and_exits(self, run_main_with_args, module_patch, sys_exit_mock, _parser_mock):
        """
        Verifies that when a command raises an exception, the script prints
        an error to stderr and exits with status 1.
        """

        args_mock = _parser_mock.return_value.parse_args.return_value
        args_mock.func.side_effect = [Exception]
        mock_stderr = module_patch("sys.stderr")

        run_main_with_args(['migrate'])

        # Check if anything was written to stderr
        mock_stderr.write.assert_called()

        # Capture the output written to stderr
        # The last call to write() should contain the critical error message
        output_call = mock_stderr.write.call_args_list[0]
        output_string = output_call.args[0]

        # Verify the specific format and content of the error message
        assert "Critical Error during execution" in output_string
        sys_exit_mock.assert_called_once()

    def test_add_migration_path(self, _cli):

        test_migration_path1 = "/path/to/migrations/1"
        test_migration_path2 = "/path/to/migrations/2"

        _cli.add_migration_path(test_migration_path1)
        _cli.add_migration_path(test_migration_path2)

        assert len(_cli.extra_migration_paths) == 2
        assert test_migration_path1 in _cli.extra_migration_paths
        assert test_migration_path2 in _cli.extra_migration_paths

    def test_add_importer_extractor(self, _cli):

        from kamadbm.importer import DataImporter, RegularImporter
        from kamadbm.extractor import DataExtractor, RegularExtractor

        class TestImporter(DataImporter): pass
        class TestExtractor(DataExtractor): pass

        assert not isinstance(_cli.get_extractor("Test"), TestExtractor)
        assert not isinstance(_cli.get_importer("Test"), TestImporter)
        assert isinstance(_cli.get_extractor("Test"), RegularExtractor)
        assert isinstance(_cli.get_importer("Test"), RegularImporter)

        _cli.add_extractor(TestExtractor())
        _cli.add_importer(TestImporter())

        assert isinstance(_cli.get_extractor("Test"), TestExtractor)
        assert isinstance(_cli.get_importer("Test"), TestImporter)

    def test_plugin_discovery(self, mocker: MockerFixture, module_patch, _cli):

        first_plugin = mocker.MagicMock()
        second_plugin = mocker.MagicMock()

        entry_points_mock = module_patch("entry_points")
        entry_points_mock.return_value = [first_plugin, second_plugin]

        _cli.run()

        entry_points_mock.assert_called_once_with(group="kama_dbm.plugins")
        first_plugin.load.assert_called_once()
        second_plugin.load.assert_called_once()
