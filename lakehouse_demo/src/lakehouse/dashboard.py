from __future__ import annotations

import subprocess
import sys

from lakehouse.config import get_project_root
from lakehouse.utils import log


def run() -> None:
    project_root = get_project_root()
    app_path = project_root / "dashboard" / "app.py"
    if not app_path.exists():
        raise FileNotFoundError(f"Dashboard app not found: {app_path}")

    log(f"Launching Streamlit dashboard: {app_path}")
    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", str(app_path)],
        check=True,
    )


if __name__ == "__main__":
    run()
