"""CLI logging starts each run with a fresh file, including old backups."""
import subprocess
import sys
from pathlib import Path


def test_cli_logger_replaces_previous_run_without_deleting_state(tmp_path):
    path = tmp_path / "engine.log"
    path.write_text("previous run\n")
    for index in (1, 2, 3):
        path.with_name(f"{path.name}.{index}").write_text("old backup")
    state = path.with_suffix(".bps-state.json")
    state.write_text('{"open_position_cost": 123}')
    result = subprocess.run(
        [sys.executable, "-c", "import logging, sys; from main import setup_logging; "
         "setup_logging('INFO', log_file=sys.argv[1]); "
         "logging.getLogger('engine').info('current run'); logging.shutdown()", str(path)],
        cwd=Path(__file__).resolve().parent.parent, capture_output=True, text=True,
        timeout=10)
    assert result.returncode == 0, result.stderr
    assert "current run" in path.read_text()
    assert "previous run" not in path.read_text()
    assert all(not path.with_name(f"{path.name}.{index}").exists() for index in (1, 2, 3))
    assert state.read_text() == '{"open_position_cost": 123}'
