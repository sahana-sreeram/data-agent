"""Tests for src/sandbox/backend.py.

TempDirSandbox is a thin wrapper around already-tested functions -- these tests just confirm
it reproduces their exact behavior. GitWorktreeSandbox tests use real `git` subprocess calls
against throwaway branches/worktrees under /tmp (never touching this repo's real working
tree or its actual branches), always cleaned up even on assertion failure.
"""

from __future__ import annotations

import subprocess

import pytest

from src.sandbox.backend import GitWorktreeSandbox, SandboxError, TempDirSandbox

# A real, small, tracked file -- safe to read-only "target" across these tests.
REAL_TARGET_FILE = "pyproject.toml"


def _real_git_available() -> bool:
    try:
        subprocess.run(["git", "rev-parse", "--is-inside-work-tree"], check=True, capture_output=True, timeout=5)
        return True
    except Exception:  # noqa: BLE001
        return False


requires_git = pytest.mark.skipif(not _real_git_available(), reason="not inside a git work tree")


# --- TempDirSandbox: byte-for-byte today's existing behavior ---------------------------------


def test_tempdir_sandbox_creates_a_workspace_containing_only_the_target_file():
    sandbox = TempDirSandbox()
    workspace_dir = sandbox.create_workspace(REAL_TARGET_FILE)
    try:
        target_path = sandbox.workspace_path(workspace_dir, REAL_TARGET_FILE)
        assert target_path.exists()
        assert target_path.read_text() == open(REAL_TARGET_FILE).read()
        # only the target file exists in the workspace -- nothing else was copied
        all_files = [p for p in workspace_dir.rglob("*") if p.is_file()]
        assert all_files == [target_path]
    finally:
        sandbox.cleanup(workspace_dir)
    assert not workspace_dir.exists()


def test_tempdir_sandbox_cleanup_is_safe_to_call_twice():
    sandbox = TempDirSandbox()
    workspace_dir = sandbox.create_workspace(REAL_TARGET_FILE)
    sandbox.cleanup(workspace_dir)
    sandbox.cleanup(workspace_dir)  # must not raise


# --- GitWorktreeSandbox -----------------------------------------------------------------------


@requires_git
def test_git_worktree_sandbox_creates_a_real_worktree_with_full_checkout():
    sandbox = GitWorktreeSandbox()
    workspace_dir = sandbox.create_workspace(REAL_TARGET_FILE)
    try:
        # a full checkout, not a single-file copy -- the target file is at its real path
        target_path = sandbox.workspace_path(workspace_dir, REAL_TARGET_FILE)
        assert target_path.exists()
        assert target_path.read_text() == open(REAL_TARGET_FILE).read()
        # this is genuinely a separate git worktree, not the main working tree
        assert (workspace_dir / ".git").exists()
        result = subprocess.run(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=workspace_dir, capture_output=True, text=True, check=True)
        assert result.stdout.strip().startswith("repair/")
    finally:
        sandbox.cleanup(workspace_dir)
    assert not workspace_dir.exists()


@requires_git
def test_git_worktree_sandbox_cleanup_removes_worktree_and_branch():
    sandbox = GitWorktreeSandbox()
    workspace_dir = sandbox.create_workspace(REAL_TARGET_FILE)
    branch = sandbox._branches_by_workspace[str(workspace_dir)]

    sandbox.cleanup(workspace_dir)

    assert not workspace_dir.exists()
    branches = subprocess.run(["git", "branch", "--list", branch], capture_output=True, text=True, check=True).stdout
    assert branch not in branches


@requires_git
def test_git_worktree_sandbox_keep_branch_preserves_the_branch(monkeypatch):
    sandbox = GitWorktreeSandbox()
    workspace_dir = sandbox.create_workspace(REAL_TARGET_FILE)
    branch = sandbox._branches_by_workspace[str(workspace_dir)]

    returned_branch = sandbox.keep_branch(workspace_dir)

    assert returned_branch == branch
    assert not workspace_dir.exists()  # the worktree checkout is gone
    branches = subprocess.run(["git", "branch", "--list", branch], capture_output=True, text=True, check=True).stdout
    assert branch in branches  # but the branch itself survives

    # cleanup the branch this test created, so it doesn't leak into the real repo
    subprocess.run(["git", "branch", "-D", branch], capture_output=True)


@requires_git
def test_git_worktree_sandbox_records_audit_log_entries():
    sandbox = GitWorktreeSandbox()
    workspace_dir = sandbox.create_workspace(REAL_TARGET_FILE)
    sandbox.cleanup(workspace_dir)

    actions = [entry.action for entry in sandbox.audit_log]
    assert actions == ["create_workspace", "cleanup"]
    assert sandbox.audit_log[0].target_file == REAL_TARGET_FILE
    assert sandbox.audit_log[0].branch is not None


def test_git_worktree_sandbox_raises_sandbox_error_when_not_a_git_repo(tmp_path):
    sandbox = GitWorktreeSandbox(repo_root=tmp_path)
    with pytest.raises(SandboxError):
        sandbox.create_workspace(REAL_TARGET_FILE)
