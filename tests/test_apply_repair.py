"""Direct unit tests for src/apply_repair.py's patch-application primitives.

_create_isolated_workspace/_validate_and_apply_patch/_workspace_path/_sha256_of_file/
load_repair_targets are exercised indirectly via tests/test_lifecycle_apply_repair.py and
tests/test_sandbox.py (their actual callers) -- no live model calls here.
"""

from __future__ import annotations

import pytest

from src.apply_repair import PatchApplyError, apply_structured_config_edit, apply_unified_diff


def test_apply_structured_config_edit_returns_new_dict_without_mutating_original():
    original = {"a": 1, "b": 2}
    patched = apply_structured_config_edit(original, [{"field": "a", "value": 99}])
    assert patched == {"a": 99, "b": 2}
    assert original == {"a": 1, "b": 2}  # unmutated


def test_apply_unified_diff_rejects_context_mismatch():
    original = "line1\nline2\nline3\n"
    diff = "--- a/f\n+++ b/f\n@@\n line1\n-wrong line\n+new line\n line3\n"
    with pytest.raises(PatchApplyError):
        apply_unified_diff(original, diff)


def test_apply_unified_diff_rejects_diff_with_no_hunks():
    with pytest.raises(PatchApplyError):
        apply_unified_diff("line1\n", "--- a/f\n+++ b/f\n")


def test_apply_unified_diff_tolerates_the_apply_patch_envelope():
    # Confirmed live: gpt-5 (via the Responses API) sometimes wraps an otherwise well-formed
    # unified diff in OpenAI's own apply_patch envelope instead of a bare unified diff --
    # these wrapper lines carry no diff content and must be skipped, not rejected.
    original = "line1\nline2\nline3\n"
    diff = "*** Begin Patch\n*** Update File: f\n@@\n line1\n-line2\n+new line\n line3\n*** End Patch"
    assert apply_unified_diff(original, diff) == "line1\nnew line\nline3\n"
