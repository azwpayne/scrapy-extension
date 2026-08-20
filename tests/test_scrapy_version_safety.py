"""Verify release artifacts exclude advisory-affected Scrapy versions."""

from __future__ import annotations

import shutil
import subprocess
import tarfile
import zipfile
from pathlib import Path

from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.version import Version

PROJECT_ROOT = Path(__file__).resolve().parents[1]
AFFECTED_SCRAPY = SpecifierSet(">=0.7,<=2.15.2")


def _scrapy_requires_dist(metadata: str) -> SpecifierSet:
    """Parse the package requirement from wheel or sdist metadata."""
    for line in metadata.splitlines():
        if not line.startswith("Requires-Dist:"):
            continue
        requirement = Requirement(line.split(":", 1)[1].strip())
        if requirement.name.lower() == "scrapy":
            return requirement.specifier
    raise AssertionError("Scrapy Requires-Dist not found in release metadata")


def _wheel_requires_dist(wheel_path: Path) -> SpecifierSet:
    """Read the Scrapy requirement from a wheel's METADATA file."""
    with zipfile.ZipFile(wheel_path) as archive:
        metadata_path = next(
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        )
        return _scrapy_requires_dist(archive.read(metadata_path).decode("utf-8"))


def _sdist_requires_dist(sdist_path: Path) -> SpecifierSet:
    """Read the Scrapy requirement from an sdist's PKG-INFO file."""
    with tarfile.open(sdist_path, "r:gz") as archive:
        metadata_member = next(
            member
            for member in archive.getmembers()
            if member.name.endswith("PKG-INFO")
        )
        extracted = archive.extractfile(metadata_member)
        assert extracted is not None
        return _scrapy_requires_dist(extracted.read().decode("utf-8"))


def _build_artifacts(out_dir: Path) -> tuple[Path, Path]:
    """Build both release artifacts into an isolated temporary directory."""
    uv_cmd = shutil.which("uv") or str(PROJECT_ROOT / ".venv" / "bin" / "uv")
    result = subprocess.run(
        [
            uv_cmd,
            "build",
            "--clear",
            "--wheel",
            "--sdist",
            "--out-dir",
            str(out_dir),
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"uv build failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )
    wheels = sorted(out_dir.glob("*.whl"))
    sdists = sorted(out_dir.glob("*.tar.gz"))
    assert len(wheels) == 1, f"Expected exactly one wheel, got {wheels}"
    assert len(sdists) == 1, f"Expected exactly one sdist, got {sdists}"
    return wheels[0], sdists[0]


def test_release_artifacts_exclude_affected_scrapy_versions(tmp_path: Path) -> None:
    """Build wheel and sdist here, then verify both carry the safe floor."""
    wheel, sdist = _build_artifacts(tmp_path / "artifacts")
    wheel_req = _wheel_requires_dist(wheel)
    sdist_req = _sdist_requires_dist(sdist)

    assert wheel_req == sdist_req
    affected_versions = (
        Version("2.14.2"),
        Version("2.15.0"),
        Version("2.15.2"),
    )
    for version in affected_versions:
        assert version in AFFECTED_SCRAPY
        assert version not in wheel_req
    assert Version("2.17.0") in wheel_req
