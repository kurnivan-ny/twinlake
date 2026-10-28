"""CI lint for rule files (no Spark needed):
    python -m twinlake.quality.lint conf/quality/*.yml
"""
import sys

from .spec import SpecError, load


def main(paths: list[str]) -> int:
    bad = 0
    for p in paths:
        try:
            spec = load(p)
            print(f"OK   {p}: {spec.table} ({len(spec.checks)} checks)")
        except SpecError as e:
            bad += 1
            print(f"FAIL {e}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
