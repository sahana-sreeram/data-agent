"""Automatic technical-context enrichment: schema introspection, structural code parsing,
lineage construction, and runtime health -- plus an optional Codex/LLM pass for anything the
deterministic extractors can't resolve. All output is Pydantic-validated
(src/context_store/models.py) and written with review_status=UNREVIEWED; nothing here is wired
into the live diagnosis/repair/Q&A path yet (see the project plan's Phase 3 notes).
"""
