"""Task-count options must not impose arbitrary default or maximum limits."""
from unittest.mock import Mock

import pytest

import web_main


@pytest.mark.parametrize("options, expected", [([], 0), (["--max-running", "0"], 0),
                                               (["--max-running", "80"], 80)])
def test_cli_accepts_unlimited_default_and_large_explicit_cap(monkeypatch, options, expected):
    monkeypatch.setattr("sys.argv", ["web_main.py", *options])
    monkeypatch.setenv("WEB_PASSWORD", "synthetic-web-test-password")
    monkeypatch.setattr(web_main, "load_dotenv", Mock())
    manager = Mock()
    monkeypatch.setattr(web_main, "TaskManager", manager)
    monkeypatch.setattr(web_main, "create_app", Mock())
    monkeypatch.setattr(web_main.web, "run_app", Mock())
    web_main.main()
    assert manager.call_args.args[2] == expected


def test_cli_rejects_negative_task_cap(monkeypatch):
    monkeypatch.setattr("sys.argv", ["web_main.py", "--max-running", "-1"])
    monkeypatch.setenv("WEB_PASSWORD", "synthetic-web-test-password")
    monkeypatch.setattr(web_main, "load_dotenv", Mock())
    manager = Mock()
    monkeypatch.setattr(web_main, "TaskManager", manager)
    with pytest.raises(SystemExit) as error:
        web_main.main()
    assert error.value.code == 2
    manager.assert_not_called()
