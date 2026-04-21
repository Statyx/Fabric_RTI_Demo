"""Tier 1 — Smoke tests for Fabric RTI Demo.

Run:  python -m pytest tests/test_smoke.py -v
"""
import py_compile
import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"

_PY_FILES = sorted(SRC.glob("*.py"))


@pytest.mark.smoke
class TestSyntax:
    @pytest.fixture(params=_PY_FILES, ids=lambda p: p.name)
    def pyfile(self, request):
        return request.param

    def test_compiles(self, pyfile):
        py_compile.compile(str(pyfile), doraise=True)


@pytest.mark.smoke
class TestConfig:
    def test_config_exists(self):
        assert (SRC / "config.yaml").exists()

    def test_config_required_keys(self):
        cfg = yaml.safe_load((SRC / "config.yaml").read_text(encoding="utf-8"))
        for key in ["workspace_name", "capacity_id", "eventhouse_name"]:
            assert key in cfg, f"Missing config key: {key}"

    def test_capacity_format(self):
        cfg = yaml.safe_load((SRC / "config.yaml").read_text(encoding="utf-8"))
        cap = cfg.get("capacity_id", "")
        assert re.match(r"^[0-9a-fA-F-]{36}$", cap), f"Bad capacity_id: {cap}"


@pytest.mark.smoke
class TestFewshots:
    def test_fewshots_valid_json(self):
        import json
        p = SRC / "agent_fewshots.json"
        if not p.exists():
            pytest.skip("No fewshots file")
        data = json.loads(p.read_text(encoding="utf-8"))
        # Fabric format: {"$schema": "...", "fewShots": [...]} or plain list
        if isinstance(data, dict):
            assert "fewShots" in data, "fewshots dict missing 'fewShots' key"
            shots = data["fewShots"]
        else:
            shots = data
        assert isinstance(shots, list), "fewshots must be a list"
        assert len(shots) > 0, "fewshots array is empty"
