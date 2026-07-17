"""
conftest.py (repo root)
-----------------------
Global pytest configuration for the FinPulse pipeline project.

This file runs before any test collection or plugin loading. It patches
out the broken `langsmith` pytest plugin which crashes pytest at startup
on this machine due to a pydantic-core version conflict (system-level
package issue, unrelated to FinPulse code).
"""

import importlib.metadata
from unittest.mock import patch

# ---------------------------------------------------------------------------
# Neutralise the langsmith pytest entry point before it is loaded.
# The crash happens inside pluggy's load_setuptools_entrypoints() when it
# tries to import langsmith, which triggers a pydantic-core version check.
#
# Strategy: patch importlib.metadata.packages_distributions / entry_points
# so that the "langsmith" group in "pytest11" is invisible to pytest.
# ---------------------------------------------------------------------------

_original_entry_points = importlib.metadata.entry_points


def _filtered_entry_points(**kwargs):
    eps = _original_entry_points(**kwargs)
    group = kwargs.get("group", "")
    if group == "pytest11":
        # eps may be a dict or a SelectableGroups object depending on Python version
        if hasattr(eps, "select"):
            # Python 3.12+ returns a SelectableGroups-like object
            filtered = [ep for ep in eps if "langsmith" not in ep.value]
            return filtered
        elif isinstance(eps, dict):
            return {k: [e for e in v if "langsmith" not in e.value]
                    for k, v in eps.items()}
        else:
            return [ep for ep in eps if "langsmith" not in ep.value]
    return eps


# Apply the patch at import time (before pluggy loads entry points)
importlib.metadata.entry_points = _filtered_entry_points
