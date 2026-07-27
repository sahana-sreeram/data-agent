"""Context storage/merge layer: automatically derived technical context + minimal
human-approved semantic annotations, resolved through one precedence-aware store.

This package is purely additive -- nothing in the live ask_lifecycle.py /
lifecycle_diagnose_pipeline.py / lifecycle_apply_repair.py path reads through it yet. It exists
alongside today's hand-authored context/*.json files, which remain authoritative for the live
system until a generated layer is proven trustworthy (see the project plan's Phase 2 notes).
"""
