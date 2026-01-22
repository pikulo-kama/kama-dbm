import pytest
from pytest_mock import MockerFixture


class BaseCommandTest:

    @pytest.fixture
    def _cli(self, mocker: MockerFixture):
        return mocker.MagicMock()

    @pytest.fixture
    def _args(self, mocker: MockerFixture):
        return mocker.MagicMock()

    @pytest.fixture
    def _context(self, db_manager_mock, _cli, _args):
        from kamadbm.command import CommandContext

        return CommandContext(
            args=_args,
            database=db_manager_mock.return_value,
            cli=_cli
        )
