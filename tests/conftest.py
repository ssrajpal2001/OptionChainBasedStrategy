import os
import sys

# Repo root = two levels up from this file (tests/conftest.py)
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
