
class TestMain:

    def test_main(self, module_patch):
        from kamadbm.main import main

        cli_mock = module_patch("DatabaseCLI")

        main()

        cli_mock.assert_called_once()
        cli_mock.return_value.run.assert_called_once()
