"""Unit tests for the alert rules: promtool replays synthetic series through the generated rules.

    ~/ba/.venv/bin/python scripts/test_rules.py [--promtool PATH]

The rules come from scripts/slo_rules.py (the same build as deploy/observability/slo-rules.yaml), the
cases from deploy/observability/tests/slo-rules.test.yaml. Annotations are left out of the rules under
test: promtool compares them as exact text, and these tests are about when alerts fire, not their wording.
"""
import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))
from slo_rules import build  # noqa: E402

TESTS = Path(__file__).resolve().parent.parent / "deploy" / "observability" / "tests" / "slo-rules.test.yaml"


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--promtool", default="promtool")
    a = ap.parse_args()
    groups = build()["spec"]["groups"]
    for g in groups:
        for r in g["rules"]:
            r.pop("annotations", None)
    with tempfile.TemporaryDirectory() as d:
        (Path(d) / "slo-rules.rules.yaml").write_text(yaml.safe_dump({"groups": groups}, sort_keys=False, width=200))
        shutil.copy(TESTS, d)
        for cmd in (["check", "rules", str(Path(d) / "slo-rules.rules.yaml")], ["test", "rules", str(Path(d) / TESTS.name)]):
            if code := subprocess.run([a.promtool, *cmd]).returncode:
                sys.exit(code)


if __name__ == "__main__":
    main()
