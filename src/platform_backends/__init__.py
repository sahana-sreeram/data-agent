"""Pluggable platform backends for pipeline execution (PipelineRunner), runtime evidence
(RuntimeInspector), and workflow state (StateStore). Local implementations reproduce today's
exact existing behavior byte-for-byte (see each module's docstring) and stay the default
everywhere -- no existing call site changes behavior by this package existing. RHOAI/
Spark-History-backed implementations are additive, selected only via src.config's
EXECUTION_BACKEND/RUNTIME_BACKEND/STATE_BACKEND env vars.
"""
