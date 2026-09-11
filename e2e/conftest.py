"""
Shared fixtures for the end-to-end stacks.

Each test module names the tool it drives; the version comes from the
environment (`EMIT_E2E_AIRFLOW=2.10.5` and so on) and the module is skipped
when its variable is unset, so `pytest e2e` with nothing set runs nothing and
CI sets exactly one.
"""

import os
import sys
from typing import Dict, Iterator

import pytest

import harness


def version_for(tool: str) -> str:
    value = os.environ.get(f"EMIT_E2E_{tool.upper()}")
    if not value:
        pytest.skip(f"EMIT_E2E_{tool.upper()} is not set")
    return value


@pytest.fixture
def stack(request: pytest.FixtureRequest) -> Iterator[harness.Stack]:
    tool: str = request.module.TOOL
    version = version_for(tool)
    env: Dict[str, str] = request.module.compose_env(version)
    stack = harness.Stack(tool, env)
    stack.down()
    try:
        yield stack
    finally:
        if request.node.rep_call_failed if hasattr(request.node, "rep_call_failed") else False:
            print("\n===== container logs =====", file=sys.stderr)
            print(stack.logs(), file=sys.stderr)
        if os.environ.get("EMIT_E2E_KEEP") != "1":
            stack.down()


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if report.when == "call":
        item.rep_call_failed = report.failed
