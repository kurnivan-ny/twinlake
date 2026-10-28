from .spec import Check, SpecError, TableSpec, load, parse  # noqa: F401

try:  # engine needs pyspark; spec works without it (CI lint)
    from .engine import DataQualityError, DQResult, register_rule, run_checks  # noqa: F401
except ImportError:  # pragma: no cover
    pass
