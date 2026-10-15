"""Reset text logs for a new run without touching trading state or CSVs."""
from pathlib import Path


def reset_run_log(path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Only numbered text-log backups belong to previous runs. In particular,
    # engine.bps-state.json contains open-position costs and must survive.
    for index in (1, 2, 3):
        path.with_name(f"{path.name}.{index}").unlink(missing_ok=True)
    with path.open("w", encoding="utf-8"):
        pass
