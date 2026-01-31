import importlib
import pytest
import sys
import os

from pytest_mock import MockerFixture


class TestKamaDBMHook:

    @pytest.fixture
    def _load_module(self):
        def load_module():
            module_name = "kamadbm._pyinstaller_hooks.hook-kamadbm"

            if module_name in sys.modules:
                return importlib.reload(sys.modules[module_name])

            return importlib.import_module(module_name)

        return load_module

    @pytest.fixture
    def mock_distribution(self, mocker: MockerFixture):
        """
        Creates a mock distribution with specific entry points and metadata.
        """

        distribution = mocker.MagicMock()
        # Mock entry point group matching

        entry_point = mocker.MagicMock()
        entry_point.group = "kama_dbm.plugins"
        distribution.entry_points = [entry_point]

        # Mock metadata and package names
        distribution.metadata = {"Name": "kama-plugin-example"}
        distribution.read_text.return_value = "kama_plugin_pkg\n"

        return distribution

    def test_plugin_discovery_and_collection(self, mocker: MockerFixture, mock_distribution, _load_module):
        """
        Tests that the hook finds plugins and adds their modules/data.
        """

        mocker.patch("importlib.metadata.distributions", return_value=[mock_distribution])
        mocker.patch("PyInstaller.utils.hooks.collect_entry_point", return_value=([], ["initial_import"]))
        mocker.patch("PyInstaller.utils.hooks.collect_submodules", return_value=["kama_plugin_pkg.sub"])
        mocker.patch("PyInstaller.utils.hooks.collect_data_files", return_value=[("data_src", "data_dst")])
        copy_metadata_mock = mocker.patch(
            "PyInstaller.utils.hooks.copy_metadata",
            return_value=[("meta_src", "meta_dst")]
        )

        hook_module = _load_module()

        assert "kama_plugin_pkg" in hook_module.hiddenimports
        assert "kama_plugin_pkg.sub" in hook_module.hiddenimports
        assert ("meta_src", "meta_dst") in hook_module.datas

        copy_metadata_mock.assert_called_with("kama-plugin-example")

    def test_binary_only_added_if_exists(self, mocker: MockerFixture, _load_module):
        """
        Ensures binaries list is empty if the exe path doesn't exist.
        """

        mocker.patch("os.path.exists", return_value=False)

        hook_module = _load_module()

        assert len(hook_module.binaries) == 0
