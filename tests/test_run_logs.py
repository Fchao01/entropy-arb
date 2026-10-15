"""CLI restarts append logs without deleting history, backups or state."""
import subprocess
import sys
from pathlib import Path

import pytest


def test_cli_logger_appends_across_restarts_without_deleting_history_or_state(tmp_path):
    path = tmp_path / "engine.log"
    path.write_text("previous run\n")
    for index in (1, 2, 3):
        path.with_name(f"{path.name}.{index}").write_text("old backup")
    state = path.with_suffix(".bps-state.json")
    state.write_text('{"open_position_cost": 123}')
    for message in ("first restart", "second restart"):
        result = subprocess.run(
            [sys.executable, "-c", "import logging, sys; from main import setup_logging; "
             "setup_logging('INFO', log_file=sys.argv[1]); "
             "logging.getLogger('engine').info(sys.argv[2]); logging.shutdown()", str(path), message],
            cwd=Path(__file__).resolve().parent.parent, capture_output=True, text=True,
            timeout=10)
        assert result.returncode == 0, result.stderr
    content = path.read_text()
    assert content.startswith("previous run\n")
    assert content.index("first restart") < content.index("second restart")
    assert all(path.with_name(f"{path.name}.{index}").read_text() == "old backup"
               for index in (1, 2, 3))
    assert state.read_text() == '{"open_position_cost": 123}'


@pytest.mark.parametrize("filename", ["engine.log", "nested/logs/engine.log"])
def test_cli_logger_creates_new_log_and_parent_directories(tmp_path, filename):
    path = tmp_path / filename
    result = subprocess.run(
        [sys.executable, "-c", "import logging, sys; from main import setup_logging; "
         "setup_logging('INFO', log_file=sys.argv[1]); "
         "logging.getLogger('engine').info('new run'); logging.shutdown()", str(path)],
        cwd=Path(__file__).resolve().parent.parent, capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    assert "new run" in path.read_text()
