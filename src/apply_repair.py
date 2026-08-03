"""Deterministic, isolated-workspace patch application shared by the lifecycle repair flow
(src/lifecycle_apply_repair.py) and the sandbox backend (src/sandbox/backend.py).

This module never decides whether a repair SUCCEEDED -- only that it was safely and correctly
APPLIED to an isolated COPY of its target file, never the real repository file.
src/lifecycle_verify_repair.py is the only thing that may mark a repair VERIFIED, by rerunning
tests/ETL/validation against that isolated workspace and comparing to the pre-repair baseline.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
from pathlib import Path

from src.repair_models import RepairPlan, RepairType

DEFAULT_REPAIR_TARGETS_FILE = "context/repair_targets.json"


class PatchApplyError(Exception):
    """Raised when a unified diff or structured edit cannot be safely, unambiguously applied.

    Caught by callers and treated as a policy BLOCK, never a crash.
    """


def _find_subsequence(haystack: list, needle: list, start: int = 0):
    """Return the index where needle occurs as a contiguous subsequence of haystack, at/after start, else None."""
    n, m = len(haystack), len(needle)
    if m == 0:
        return start
    for i in range(start, n - m + 1):
        if haystack[i : i + m] == needle:
            return i
    return None


def apply_unified_diff(original_text: str, diff_text: str) -> str:
    """Apply a single-file unified diff, returning the patched text.

    Each hunk is located by the CONTENT of its context/removed lines, not by
    the line numbers declared in its "@@ ... @@" header -- models frequently
    omit those numbers (a bare "@@") or miscount them. This is deliberately
    more like a content-anchored patch than a strict line-oriented one: every
    context/removed line must still exactly match somewhere in the original,
    in order, or this raises PatchApplyError. No fuzzy/approximate matching
    beyond exact line content. Supports multiple hunks. Pure Python -- no
    subprocess, no dependency on a system `patch` binary.
    """
    original_lines = original_text.splitlines()
    trailing_newline = original_text.endswith("\n")

    hunks: list = []
    in_hunk = False
    for line in diff_text.splitlines():
        if line.startswith("@@"):
            hunks.append([])
            in_hunk = True
            continue
        if line.startswith("---") or line.startswith("+++"):
            continue
        # A model asked for a "unified diff" sometimes wraps it in OpenAI's own apply_patch
        # envelope instead ("*** Begin Patch" / "*** Update File: ..." / "*** End Patch") --
        # confirmed live from gpt-5 via the Responses API. None of these carry diff content
        # (a real diff line is always blank or starts with " "/"-"/"+"/"\\"), so skip them
        # wherever they appear rather than rejecting an otherwise-valid, well-formed patch.
        if line.startswith("***"):
            continue
        if in_hunk:
            hunks[-1].append(line)

    if not hunks:
        raise PatchApplyError("no hunks found in unified diff (expected at least one '@@ ... @@' hunk header)")

    result: list = []
    cursor = 0  # 0-indexed position in original_lines

    for body in hunks:
        anchor = [ln[1:] for ln in body if ln and ln[0] in (" ", "-")]
        if anchor:
            match_index = _find_subsequence(original_lines, anchor, start=cursor)
            if match_index is None:
                raise PatchApplyError(f"could not locate hunk context in original file (starting {anchor[:2]!r})")
        else:
            match_index = cursor  # pure-insertion hunk: applies wherever the cursor currently is

        result.extend(original_lines[cursor:match_index])
        cursor = match_index

        for body_line in body:
            if body_line.startswith("\\"):
                continue  # "\ No newline at end of file" marker -- ignore
            tag, content = (body_line[0], body_line[1:]) if body_line else (" ", "")

            if tag == " ":
                if cursor >= len(original_lines) or original_lines[cursor] != content:
                    found = original_lines[cursor] if cursor < len(original_lines) else "<EOF>"
                    raise PatchApplyError(
                        f"context mismatch at original line {cursor + 1}: expected {content!r}, found {found!r}"
                    )
                result.append(content)
                cursor += 1
            elif tag == "-":
                if cursor >= len(original_lines) or original_lines[cursor] != content:
                    found = original_lines[cursor] if cursor < len(original_lines) else "<EOF>"
                    raise PatchApplyError(
                        f"removal mismatch at original line {cursor + 1}: expected {content!r}, found {found!r}"
                    )
                cursor += 1
            elif tag == "+":
                result.append(content)
            else:
                raise PatchApplyError(f"unrecognized diff line: {body_line!r}")

    result.extend(original_lines[cursor:])
    patched = "\n".join(result)
    if trailing_newline:
        patched += "\n"
    return patched


def apply_structured_config_edit(original_content: dict, operations: list) -> dict:
    """Apply a small, already-validated list of {field, value} operations to a JSON object."""
    patched = dict(original_content)
    for op in operations:
        patched[op["field"]] = op["value"]
    return patched


def load_json(path: Path, label: str) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"{label} file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_repair_targets(path: Path) -> dict:
    return load_json(path, "repair targets")["targets"]


def _sha256_of_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _workspace_path(workspace_dir: Path, target_file: str) -> Path:
    """Map a target_file (relative or absolute) to its mirrored location under workspace_dir.

    Joining a Path with an absolute string DISCARDS the base (a pathlib
    footgun: Path("/tmp/x") / "/abs/y" == Path("/abs/y")) -- stripping the
    leading separator guarantees the result always lands under workspace_dir,
    never silently aliasing back to the real file.
    """
    return workspace_dir / target_file.lstrip("/")


def _create_isolated_workspace(target_file: str) -> Path:
    """A temp directory containing a copy of ONLY the target file, mirroring its repo-relative path."""
    workspace_dir = Path(tempfile.mkdtemp(prefix="repair_workspace_"))
    dest_path = _workspace_path(workspace_dir, target_file)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(Path(target_file), dest_path)
    return workspace_dir


def _validate_and_apply_patch(workspace_dir: Path, plan: RepairPlan) -> None:
    """Deterministic policy validation that needs real file content, plus the actual apply.

    Raises PatchApplyError on any policy violation -- callers treat this as
    repair_status=BLOCKED, not a crash.
    """
    target_path = _workspace_path(workspace_dir, plan.target_file)

    if plan.repair_type == RepairType.CODE_CHANGE:
        original_text = target_path.read_text(encoding="utf-8")
        patched_text = apply_unified_diff(original_text, plan.patch.content)
        try:
            compile(patched_text, str(target_path), "exec")
        except SyntaxError as exc:
            raise PatchApplyError(f"patched {plan.target_file} is not valid Python: {exc}") from exc
        if patched_text == original_text:
            raise PatchApplyError("patch produced no actual change")
        target_path.write_text(patched_text, encoding="utf-8")

    elif plan.repair_type == RepairType.CONFIGURATION_CHANGE:
        original_content = json.loads(target_path.read_text(encoding="utf-8"))
        patched_content = apply_structured_config_edit(original_content, plan.patch.content["operations"])
        if patched_content == original_content:
            raise PatchApplyError("patch produced no actual change")
        with target_path.open("w", encoding="utf-8") as f:
            json.dump(patched_content, f, indent=2)
            f.write("\n")

    else:
        raise PatchApplyError(f"unsupported repair_type for application: {plan.repair_type}")
