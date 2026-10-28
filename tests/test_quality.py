"""Run locally:  pip install pysail "pyspark[connect]" pyyaml pytest && pytest -q

Starts an in-process Sail server, so no JVM or cluster is needed.
The same tests pass on Databricks by swapping the `spark` fixture.
"""
import datetime as dt

import pytest

from twinlake.quality import DataQualityError, parse, run_checks

D = dt.date


@pytest.fixture(scope="session")
def spark():
    from pysail.spark import SparkConnectServer
    from pyspark.sql import SparkSession

    server = SparkConnectServer()
    server.start(background=True)
    _, port = server.listening_address
    session = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    yield session
    session.stop()
    server.stop()


@pytest.fixture
def orders(spark):
    rows = [
        # order_id, order_date,   ship_date,   amount, status,  email
        (1, D(2026, 10, 1), D(2026, 10, 2), 100.0, "PAID",   "a@x.com"),   # clean
        (2, D(2026, 10, 1), None,           -5.0,  "NEW",    "b@x.com"),   # negative amount
        (3, D(2026, 10, 3), D(2026, 10, 1), 20.0,  "PAID",   "c@x.com"),   # ship before order
        (4, None,           None,           10.0,  "LOST",   "bad-email"), # null date, bad status, bad email
        (5, D(2026, 10, 4), None,           30.0,  "NEW",    "nope"),      # warn only (email)
        (5, D(2026, 10, 4), None,           30.0,  "NEW",    "e@x.com"),   # duplicate key
    ]
    cols = "order_id INT, order_date DATE, ship_date DATE, amount DOUBLE, status STRING, email STRING"
    return spark.createDataFrame(rows, cols)


SPEC = {
    "table": "silver.sales",
    "fail_if_quarantine_ratio_above": 0.9,
    "checks": [
        {"name": "not_null", "rule": "not_null", "columns": ["order_id", "order_date"]},
        {"name": "amount_ok", "rule": "between", "columns": ["amount"], "params": {"min": 0}},
        {"name": "status_ok", "rule": "in_set", "columns": ["status"],
         "params": {"values": ["NEW", "PAID", "SHIPPED", "CANCELLED"]}},
        {"name": "email_ok", "rule": "regex", "columns": ["email"],
         "params": {"pattern": r"^[^@\s]+@[^@\s]+\.[^@\s]+$"}, "severity": "warn"},
        {"name": "ship_after_order", "rule": "expr",
         "params": {"expr": "ship_date IS NULL OR ship_date >= order_date"}},
        {"name": "unique_id", "rule": "unique", "columns": ["order_id"], "severity": "warn"},
        {"name": "not_empty", "rule": "row_count", "params": {"min": 1}},
    ],
}


def test_row_level_split(orders):
    r = run_checks(orders, parse(SPEC))
    assert r.total_rows == 6
    assert r.quarantined_rows == 3                       # ids 2, 3, 4
    assert sorted(x.order_id for x in r.valid.collect()) == [1, 5, 5]
    reasons = {x.order_id: set(x._dq_errors.split(",")) for x in r.quarantine.collect()}
    assert reasons[2] == {"amount_ok"}
    assert reasons[3] == {"ship_after_order"}
    assert reasons[4] == {"not_null", "status_ok"}
    assert "_dq_errors" not in r.valid.columns


def test_summary_counts(orders):
    s = {x["check"]: x for x in run_checks(orders, parse(SPEC)).summary}
    assert s["email_ok"]["failed_rows"] == 2 and s["email_ok"]["severity"] == "warn"
    assert s["unique_id"]["failed"] is True
    assert s["not_empty"]["failed"] is False


def test_warn_does_not_fail_job(orders):
    run_checks(orders, parse(SPEC)).raise_if_failed()     # no exception


def test_dataset_error_fails_job(orders):
    spec = dict(SPEC, checks=SPEC["checks"][:-2] + [
        {"name": "unique_id", "rule": "unique", "columns": ["order_id"]}])
    with pytest.raises(DataQualityError, match="unique_id"):
        run_checks(orders, parse(spec)).raise_if_failed()


def test_ratio_threshold_fails_job(orders):
    spec = dict(SPEC, fail_if_quarantine_ratio_above=0.1)
    with pytest.raises(DataQualityError, match="quarantine ratio"):
        run_checks(orders, parse(spec)).raise_if_failed()
