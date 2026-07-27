"""Pluggable repair-sandbox backends. TempDirSandbox reproduces today's exact isolation
behavior (src.legacy.apply_repair's tempfile.mkdtemp()/shutil.copy2) byte-for-byte and stays
the default everywhere, so no existing call site or test changes behavior by importing this
package. GitWorktreeSandbox is the new, stronger option -- a real git branch/worktree instead
of a bare temp copy -- used by callers that opt into it (e.g. a future `create_pr` repair
mode, which inherently needs a real branch anyway).
"""
