import subprocess

import pytest


def _is_generated_python_artifact(path: str) -> bool:
    return "__pycache__/" in path or path.endswith(".pyc") or ".egg-info/" in path


def test_generated_python_artifacts_are_not_tracked() -> None:
    result = subprocess.run(
        ["git", "ls-files"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.skip("git metadata is unavailable")

    tracked_files = result.stdout.splitlines()
    offenders = [
        path
        for path in tracked_files
        if _is_generated_python_artifact(path)
    ]

    assert offenders == []
