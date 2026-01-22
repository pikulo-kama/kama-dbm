import pytest
from pytest_mock import MockerFixture


class TestCLICommand:

    @pytest.fixture
    def _cli_mock(self, mocker: MockerFixture):
        """
        Provides a mocked DatabaseCLI instance.
        """
        return mocker.MagicMock()

    @pytest.fixture
    def _execute_command_mock(self, mocker: MockerFixture):
        return mocker.MagicMock()

    @pytest.fixture
    def _command(self, _execute_command_mock, _cli_mock):
        """
        Provides an instance of CLICommand (or a subclass) for testing.
        """

        from kamadbm.command import CLICommand

        # We can't test _execute_command on the base class directly
        # as it's a pass, so we'll watch the call to it.
        command = CLICommand(_cli_mock)
        command._execute_command = _execute_command_mock

        return command

    @pytest.fixture
    def _args(self, mocker: MockerFixture):
        args_mock = mocker.MagicMock()
        args_mock.database = "/path/to/db.sqlite"

        return args_mock

    def test_execute_success(self, _execute_command_mock, module_patch, _command, _cli_mock, _args):
        """
        Tests that execute creates a context and calls _execute_command.
        """

        from kamadbm.command import CommandContext

        db_manager_mock = module_patch("DatabaseManager")

        _command.execute(_args)

        # Verify DatabaseManager was called with the path
        db_manager_mock.assert_called_once_with(_args.database)

        # Verify _execute_command was called with a CommandContext
        _execute_command_mock.assert_called_once()
        context = _execute_command_mock.call_args[0][0]

        assert isinstance(context, CommandContext)
        assert context.args == _args
        assert context.cli == _cli_mock

    def test_execute_missing_database(self, _command, _args):
        """
        Tests that the program exits if --database is missing.
        """

        _args.database = None

        # Verify that sys.exit(1) is called
        with pytest.raises(SystemExit) as error:
            _command.execute(_args)

        assert error.value.code == 1

    def test_database_path_expansion(self, module_patch, _command, _args):
        """
        Tests that environment variables in the database path are expanded.
        """

        _args.database = "$HOME/test.db"

        module_patch("DatabaseManager")
        expand_mock = module_patch("os.path.expandvars", return_value="/home/user/test.db")

        _command.execute(_args)
        expand_mock.assert_called_with("$HOME/test.db")
