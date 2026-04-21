"""Report visual feedback loop — validates build_report_json() output.

Run:  python -m pytest tests/test_report_visuals.py -v
"""
import json
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
BRAIN = ROOT.parent / "Github_Brain"
sys.path.insert(0, str(SRC))
sys.path.insert(0, str(BRAIN / "agents" / "testing-agent"))

from visual_validator import ReportValidator


def _load_config():
    return yaml.safe_load((SRC / "config.yaml").read_text(encoding="utf-8"))


def _load_state():
    p = SRC / "state.json"
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return {
        "workspace_id": "00000000-0000-0000-0000-000000000000",
        "semantic_model_id": "00000000-0000-0000-0000-000000000002",
    }


@pytest.fixture(scope="module")
def report_tuple():
    """Build report in-memory (no Azure needed)."""
    from deploy_report import build_report_json
    state = _load_state()
    cfg = _load_config()
    return build_report_json(state, cfg)


@pytest.fixture(scope="module")
def report(report_tuple):
    return report_tuple[0]


@pytest.fixture(scope="module")
def validator(report):
    v = ReportValidator(report)
    v.validate_all()
    return v


# ══════════════════════════════════════════════════════════════
#  FULL VALIDATION — zero errors allowed
# ══════════════════════════════════════════════════════════════

@pytest.mark.smoke
class TestReportVisualFeedback:
    def test_no_errors(self, validator):
        errs = validator.errors()
        if errs:
            lines = [f"  [{e.page}] {e.visual}: {e.check} — {e.message}" for e in errs]
            pytest.fail(f"{len(errs)} visual errors found:\n" + "\n".join(lines))

    def test_warnings_count(self, validator):
        warns = validator.warnings()
        if warns:
            for w in warns:
                print(f"  WARN [{w.page}] {w.visual}: {w.check} — {w.message}")


# ══════════════════════════════════════════════════════════════
#  PROTOTYPE QUERY — the #1 blank-visual cause
# ══════════════════════════════════════════════════════════════

DATA_TYPES = {
    "cardVisual", "clusteredBarChart", "clusteredColumnChart",
    "lineChart", "scatterChart", "tableEx", "slicer",
}


@pytest.mark.smoke
class TestPrototypeQuery:
    @pytest.fixture(scope="class")
    def all_data_visuals(self, report):
        result = []
        for section in report["sections"]:
            page = section.get("displayName", section.get("name", "?"))
            for vc in section.get("visualContainers", []):
                cfg = json.loads(vc.get("config", "{}"))
                sv = cfg.get("singleVisual", {})
                vtype = sv.get("visualType", "")
                if vtype in DATA_TYPES:
                    result.append({
                        "page": page, "name": cfg.get("name", "?"),
                        "type": vtype, "sv": sv,
                    })
        return result

    def test_all_have_prototype_query(self, all_data_visuals):
        missing = [f'[{v["page"]}] {v["name"]} ({v["type"]})'
                   for v in all_data_visuals if "prototypeQuery" not in v["sv"]]
        assert not missing, (
            f"{len(missing)} data visuals missing prototypeQuery (BLANK):\n"
            + "\n".join(missing)
        )

    def test_alias_references_valid(self, all_data_visuals):
        bad = []
        for v in all_data_visuals:
            pq = v["sv"].get("prototypeQuery", {})
            if not pq:
                continue
            from_aliases = {f.get("Name") for f in pq.get("From", [])}
            for sel in pq.get("Select", []):
                for key in ("Measure", "Column"):
                    if key in sel:
                        src = (sel[key].get("Expression", {})
                               .get("SourceRef", {}).get("Source"))
                        if src and src not in from_aliases:
                            bad.append(
                                f'[{v["page"]}] {v["name"]}: alias "{src}" '
                                f'not in {from_aliases}')
        assert not bad, "\n".join(bad)


# ══════════════════════════════════════════════════════════════
#  OVERLAP & BOUNDS
# ══════════════════════════════════════════════════════════════

@pytest.mark.smoke
class TestVisualPositions:
    @pytest.fixture(scope="class")
    def pages_visuals(self, report):
        result = {}
        for section in report["sections"]:
            page = section.get("displayName", section.get("name", "?"))
            visuals = []
            for vc in section.get("visualContainers", []):
                cfg = json.loads(vc.get("config", "{}"))
                sv = cfg.get("singleVisual", {})
                visuals.append({
                    "name": cfg.get("name", "?"),
                    "type": sv.get("visualType", "unknown"),
                    "x": vc["x"], "y": vc["y"],
                    "w": vc["width"], "h": vc["height"],
                    "z": vc.get("z", 0),
                })
            result[page] = visuals
        return result

    def test_no_out_of_bounds(self, pages_visuals):
        errors = []
        for pg, visuals in pages_visuals.items():
            for v in visuals:
                if v["x"] + v["w"] > 1280:
                    errors.append(f'[{pg}] {v["name"]} right={v["x"]+v["w"]}')
                if v["y"] + v["h"] > 720:
                    errors.append(f'[{pg}] {v["name"]} bottom={v["y"]+v["h"]}')
        assert not errors, "\n".join(errors)

    def test_no_overlaps(self, pages_visuals):
        decorative = {"textbox", "basicShape"}
        errors = []
        for pg, visuals in pages_visuals.items():
            content = [v for v in visuals
                       if v["type"] not in decorative and v["z"] > 0]
            for i in range(len(content)):
                for j in range(i + 1, len(content)):
                    a, b = content[i], content[j]
                    ox = min(a["x"]+a["w"], b["x"]+b["w"]) - max(a["x"], b["x"])
                    oy = min(a["y"]+a["h"], b["y"]+b["h"]) - max(a["y"], b["y"])
                    if ox > 2 and oy > 2:
                        errors.append(
                            f'[{pg}] {a["name"]} ∩ {b["name"]} ({ox}×{oy}px)')
        assert not errors, "\n".join(errors)


@pytest.mark.smoke
class TestReportFormat:
    def test_pbir_v2_schema(self, report_tuple):
        pbir = report_tuple[1]
        assert "2.0.0" in pbir.get("$schema", "")

    def test_config_stringified(self, report):
        assert isinstance(report["config"], str)
        json.loads(report["config"])
