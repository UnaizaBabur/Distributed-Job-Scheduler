"""
============================================================================
 test_consumer_logic.py
============================================================================
 Unit tests that run WITHOUT touching AWS or RDS.
 We exercise the bits of the consumer that have non-trivial logic:
   * compute_backoff_seconds — must be monotonic and capped
   * run_job dispatcher    — must accept known types & raise on unknown

 To run:
   pytest tests/
============================================================================
"""

import os
import sys
import importlib

# Stub env vars so the consumer module imports cleanly without AWS.
os.environ.setdefault("DB_HOST", "stub")
os.environ.setdefault("DB_NAME", "stub")
os.environ.setdefault("DB_USER", "stub")
os.environ.setdefault("DB_PASSWORD", "stub")
os.environ.setdefault("QUEUE_URL", "stub")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lambdas", "consumer"))


def _import():
    return importlib.import_module("consumer_lambda")


def test_backoff_is_monotonic_and_capped():
    cl = _import()
    prev = 0
    for rc in range(1, 8):
        # Run several times because of jitter
        vals = [cl.compute_backoff_seconds(rc) for _ in range(20)]
        avg = sum(vals) / len(vals)
        assert all(v <= 900 for v in vals), "backoff must be capped at 900s"
        # Allow some jitter overlap but the centre of mass should grow
        if rc > 1:
            assert avg >= prev * 0.9, f"backoff should generally grow, rc={rc}"
        prev = avg


def test_run_job_known_types():
    cl = _import()
    assert cl.run_job("echo", {"x": 1}) == {"echoed": {"x": 1}}
    res = cl.run_job("compute", {"n": 10})
    assert res["sum_of_squares"] == sum(i * i for i in range(10))
    assert res["n"] == 10


def test_run_job_unknown_raises():
    cl = _import()
    try:
        cl.run_job("not_a_real_type", {})
    except ValueError as e:
        assert "Unknown job_type" in str(e)
    else:
        raise AssertionError("expected ValueError for unknown job_type")


def test_run_job_always_fail_raises():
    cl = _import()
    try:
        cl.run_job("always_fail", {})
    except RuntimeError:
        return
    raise AssertionError("expected always_fail to raise")
