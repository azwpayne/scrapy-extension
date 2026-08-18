"""Validate the bounded dependency-audit waiver before CI applies it."""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

from packaging.specifiers import SpecifierSet
from packaging.version import Version

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 compatibility
    import tomli as tomllib


class WaiverPolicyError(ValueError):
    """The audit waiver is stale, incomplete, or inconsistent with the lock."""


def _read_toml(path: Path) -> dict[str, Any]:
    with path.open("rb") as stream:
        return tomllib.load(stream)


def _locked_versions(lock_path: Path) -> dict[str, str]:
    lock = _read_toml(lock_path)
    return {
        package["name"].lower(): package["version"]
        for package in lock["package"]
        if "version" in package
    }


def validate_waivers(root: Path, *, today: date) -> list[str]:
    """Return approved advisory IDs after validating policy, lock, and fixture."""
    policy = _read_toml(root / ".github" / "audit-waivers.toml")
    if policy.get("version") != 1:
        raise WaiverPolicyError("unsupported audit-waiver policy version")

    waivers = policy.get("waivers")
    if not isinstance(waivers, list) or not waivers:
        raise WaiverPolicyError("at least one documented waiver is required")

    locked_versions = _locked_versions(root / "uv.lock")
    approved: list[str] = []
    for waiver in waivers:
        required = {
            "advisory",
            "package",
            "affected-range",
            "waived-lock-range",
            "rationale",
            "owner",
            "expires",
            "evidence-fixture",
        }
        missing = required - waiver.keys()
        if missing:
            raise WaiverPolicyError(f"waiver is missing fields: {sorted(missing)}")

        advisory = waiver["advisory"]
        package = waiver["package"].lower()
        owner = waiver["owner"]
        rationale = waiver["rationale"]
        expires = waiver["expires"]
        if not isinstance(advisory, str) or not advisory:
            raise WaiverPolicyError("waiver advisory must be non-empty")
        if advisory in approved:
            raise WaiverPolicyError(f"duplicate waiver for {advisory}")
        if not isinstance(owner, str) or not owner.startswith("@"):
            raise WaiverPolicyError(f"{advisory} must have an accountable @owner")
        if not isinstance(rationale, str) or len(rationale) < 40:
            raise WaiverPolicyError(f"{advisory} rationale is not substantive")
        if not isinstance(expires, date):
            raise WaiverPolicyError(f"{advisory} expiry must be a TOML date")
        if today >= expires:
            raise WaiverPolicyError(
                f"{advisory} expired on {expires.isoformat()}; remove it or renew "
                "with fresh evidence in a new atomic commit"
            )

        locked = locked_versions.get(package)
        if locked is None:
            raise WaiverPolicyError(f"{package} is absent from uv.lock")
        locked_version = Version(locked)
        affected = SpecifierSet(waiver["affected-range"])
        waived_lock = SpecifierSet(waiver["waived-lock-range"])
        if locked_version in affected:
            raise WaiverPolicyError(
                f"locked {package} {locked} is in the documented affected range"
            )
        if locked_version not in waived_lock:
            raise WaiverPolicyError(
                f"locked {package} {locked} left waiver range {waived_lock}"
            )

        fixture_path = root / waiver["evidence-fixture"]
        fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
        expected = {
            "advisory": advisory,
            "package": package,
            "reported_version": locked,
            "affected_range": waiver["affected-range"],
            "captured_for_lock": "uv.lock",
        }
        mismatches = {
            key: (fixture.get(key), value)
            for key, value in expected.items()
            if fixture.get(key) != value
        }
        if mismatches:
            raise WaiverPolicyError(
                f"{advisory} locked fixture does not match policy/lock: {mismatches}"
            )
        approved.append(advisory)

    return approved


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    try:
        approved = validate_waivers(root, today=date.today())
    except (KeyError, OSError, ValueError, json.JSONDecodeError) as error:
        print(f"audit waiver policy failed: {error}", file=sys.stderr)
        return 1

    if len(approved) != 1:
        print(
            "audit waiver policy failed: CI supports exactly one waiver",
            file=sys.stderr,
        )
        return 1
    print(approved[0])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
