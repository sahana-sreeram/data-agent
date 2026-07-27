"""SandboxBackend Protocol plus two implementations.

TempDirSandbox wraps src.legacy.apply_repair's existing
_create_isolated_workspace/_workspace_path functions exactly as they are today -- a temp
directory containing a copy of ONLY the target file. It is the default everywhere in this
codebase and its behavior must never change.

GitWorktreeSandbox is a real git worktree on a throwaway branch: a full checkout at HEAD,
isolated from the main working tree, with its own branch that a `create_pr` repair mode can
commit to directly. It enforces the boundaries the project's sandboxing requirements call for:

- read-only access to raw data (nothing in this codebase's repair path ever writes to
  data/*, raw/*, or any S3 raw/curated key from *inside* a sandbox -- only the deterministic
  Python code in lifecycle_verify_repair.py promotes candidate output, and only after every
  check passes)
- write access restricted to the target file (enforced by the CALLER -- apply_unified_diff/
  apply_structured_config_edit only ever write to workspace_path(workspace_dir, target_file);
  the sandbox backend does not hand the model direct filesystem access at all, matching
  src.legacy.repair_tools's "the model never receives a write-capable tool" design)
- a bounded creation timeout (`timeout_seconds`, passed to the underlying `git` subprocess)
- an audit log entry for every workspace created and cleaned up

Real OS-level resource limits (memory/CPU caps, network isolation) are out of scope for a
pure-Python sandbox and would need a container/cgroup boundary underneath this -- documented
here rather than silently implied.
"""

from __future__ import annotations

import shutil
import subprocess
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from src.legacy.apply_repair import _create_isolated_workspace, _workspace_path

DEFAULT_TIMEOUT_SECONDS = 60.0


class SandboxError(Exception):
    """Raised when a sandbox workspace can't be created or torn down."""


class SandboxBackend(Protocol):
    def create_workspace(self, target_file: str) -> Path: ...

    def workspace_path(self, workspace_dir: Path, target_file: str) -> Path: ...

    def cleanup(self, workspace_dir: Path) -> None: ...


class TempDirSandbox:
    """Default backend -- byte-for-byte today's existing behavior. No audit log: this
    predates the audit-logging requirement and changing its observable behavior at all,
    even by adding logging, is out of scope for what's meant to be a zero-risk default."""

    def create_workspace(self, target_file: str) -> Path:
        return _create_isolated_workspace(target_file)

    def workspace_path(self, workspace_dir: Path, target_file: str) -> Path:
        return _workspace_path(workspace_dir, target_file)

    def cleanup(self, workspace_dir: Path) -> None:
        shutil.rmtree(workspace_dir, ignore_errors=True)


@dataclass
class AuditLogEntry:
    action: str  # "create_workspace" | "cleanup"
    target_file: str | None
    branch: str | None
    workspace_dir: str
    timestamp: float


@dataclass
class GitWorktreeSandbox:
    repo_root: Path = field(default_factory=lambda: Path("."))
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    audit_log: list[AuditLogEntry] = field(default_factory=list)
    _branches_by_workspace: dict = field(default_factory=dict, repr=False)

    def create_workspace(self, target_file: str) -> Path:
        run_id = uuid.uuid4().hex[:12]
        branch = f"repair/{run_id}"
        worktree_dir = Path(tempfile.mkdtemp(prefix="repair_worktree_"))
        # Remove the empty dir git worktree add would otherwise refuse to reuse.
        worktree_dir.rmdir()
        try:
            subprocess.run(
                ["git", "worktree", "add", "-b", branch, str(worktree_dir), "HEAD"],
                cwd=self.repo_root,
                check=True,
                capture_output=True,
                timeout=self.timeout_seconds,
                text=True,
            )
        except subprocess.CalledProcessError as exc:
            raise SandboxError(f"git worktree add failed: {exc.stderr}") from exc
        except subprocess.TimeoutExpired as exc:
            raise SandboxError(f"git worktree add exceeded {self.timeout_seconds}s timeout") from exc

        self._branches_by_workspace[str(worktree_dir)] = branch
        self.audit_log.append(
            AuditLogEntry(action="create_workspace", target_file=target_file, branch=branch, workspace_dir=str(worktree_dir), timestamp=time.time())
        )
        return worktree_dir

    def workspace_path(self, workspace_dir: Path, target_file: str) -> Path:
        # A git worktree is a full checkout -- the target file already lives at its real
        # repo-relative path, same mapping as TempDirSandbox for a consistent caller API.
        return workspace_dir / target_file.lstrip("/")

    def cleanup(self, workspace_dir: Path) -> None:
        branch = self._branches_by_workspace.pop(str(workspace_dir), None)
        try:
            subprocess.run(
                ["git", "worktree", "remove", "--force", str(workspace_dir)],
                cwd=self.repo_root,
                check=True,
                capture_output=True,
                timeout=self.timeout_seconds,
                text=True,
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            shutil.rmtree(workspace_dir, ignore_errors=True)  # best-effort fallback
        if branch:
            subprocess.run(
                ["git", "branch", "-D", branch], cwd=self.repo_root, capture_output=True, timeout=self.timeout_seconds
            )
        self.audit_log.append(
            AuditLogEntry(action="cleanup", target_file=None, branch=branch, workspace_dir=str(workspace_dir), timestamp=time.time())
        )

    def keep_branch(self, workspace_dir: Path) -> str | None:
        """For create_pr mode: remove the worktree's checkout but keep its branch/commits
        intact (unlike cleanup(), which deletes both) so a PR artifact can reference real,
        inspectable commits. Returns the branch name, or None if this workspace is unknown."""
        branch = self._branches_by_workspace.pop(str(workspace_dir), None)
        try:
            subprocess.run(
                ["git", "worktree", "remove", "--force", str(workspace_dir)],
                cwd=self.repo_root,
                check=True,
                capture_output=True,
                timeout=self.timeout_seconds,
                text=True,
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            shutil.rmtree(workspace_dir, ignore_errors=True)
        self.audit_log.append(
            AuditLogEntry(action="keep_branch", target_file=None, branch=branch, workspace_dir=str(workspace_dir), timestamp=time.time())
        )
        return branch
