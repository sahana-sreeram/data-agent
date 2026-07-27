"""Context storage/merge layer: automatically derived technical context + minimal
human-approved semantic annotations, resolved through one precedence-aware store.

src.context_retriever.ContextRetriever is how live agent code (Q&A, diagnosis, repair,
verification reporting) reads through this layer. As of this vertical slice, only
loan_portfolio has generated + human context populated (context/generated/loan_portfolio.json,
context/human/loan_portfolio.yaml) -- every other pipeline still resolves through
ContextRetriever's legacy-file fallback, reading today's hand-authored context/*.json files
exactly as it always did. Migrating a pipeline onto this layer is a data change (run
`python3 -m src.context_enrichment.cli --pipeline <name>`, author its
context/human/<name>.yaml), not an agent-code change.
"""
