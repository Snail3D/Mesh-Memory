#!/usr/bin/env python3
"""
Copied test harness for PR convenience. This mirrors `tests/run_ai_integration_test.py`
from the project root. Use this copy to include the test in your fork/PR.
"""

from pathlib import Path
ROOT_TEST = Path(__file__).resolve().parents[2] / 'tests' / 'run_ai_integration_test.py'
print('This is a placeholder proxy pointing to:', ROOT_TEST)
