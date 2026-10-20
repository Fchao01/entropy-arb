"""Never let test workers consume the desktop/server's live request budget."""
import pytest


@pytest.fixture(autouse=True)
def isolated_request_coordinator(tmp_path, monkeypatch):
    monkeypatch.setenv('ENTROPY_REST_DIR', str(tmp_path / 'request-coordinator'))
