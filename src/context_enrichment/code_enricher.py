"""Structural (regex-over-source-text) extraction of joins, filters, and business-rule
references from a pipeline's ETL source, plus an optional Codex/LLM pass for the parts a
regex can't resolve (grain, caveats, human-readable derived-metric formulas).

The structural extractor is the same documented tradeoff as
lifecycle_diagnostic_tools.compare_metric_definition_to_etl: fast, deterministic, and
"good enough to point an investigation in the right direction," not a real AST/semantic
analysis. It never guesses -- anything it can't confidently extract is simply omitted, left
for the Codex pass or a human to fill in.
"""

from __future__ import annotations

import inspect
import re

from src.context_store.models import FilterInfo, GeneratedContext, JoinInfo, PipelineMetadata
from src.lifecycle_pipeline_registry import PIPELINE_REGISTRY
from src.model_client import DiagnosisModelClient, ModelClientError

_JOIN_PATTERN = re.compile(
    r"""\.join\(\s*
        (?P<right>[\w.]+)\s*,\s*
        on\s*=\s*(?P<on>(?:\[[^\]]*\]|["'][\w]+["']))\s*,\s*
        how\s*=\s*["'](?P<how>\w+)["']
    """,
    re.VERBOSE | re.DOTALL,
)

_FILTER_PATTERN = re.compile(
    # Allows one level of nested parens (e.g. .isin(...) inside .filter(...)) -- still not a
    # real parser, so deeper nesting or multi-line expressions may not extract cleanly.
    r"""\.(?:filter|where)\(\s*(?P<expr>(?:[^()\n]|\([^()]*\))+)\s*\)""",
)

_BUSINESS_RULE_KEY_PATTERN = re.compile(
    r"""business_rules(?:\.get\(\s*["'](?P<get_key>\w+)["']|\[\s*["'](?P<bracket_key>\w+)["']\s*\])"""
)


def _parse_on_clause(raw_on: str) -> list[str]:
    raw_on = raw_on.strip()
    if raw_on.startswith("["):
        return re.findall(r"""["'](\w+)["']""", raw_on)
    return [raw_on.strip("'\"")]


def extract_joins(source: str) -> list[JoinInfo]:
    joins = []
    for match in _JOIN_PATTERN.finditer(source):
        joins.append(
            JoinInfo(
                left="?",  # the left side of a chained/piped join isn't reliably recoverable
                # from source text alone without a real AST walk -- left as a placeholder
                # a human or the Codex pass can fill in from the surrounding context.
                right=match.group("right"),
                on=_parse_on_clause(match.group("on")),
                how=match.group("how"),
            )
        )
    return joins


def extract_filters(source: str) -> list[FilterInfo]:
    return [FilterInfo(expression=match.group("expr")) for match in _FILTER_PATTERN.finditer(source)]


def extract_business_rule_references(source: str) -> list[str]:
    keys = set()
    for match in _BUSINESS_RULE_KEY_PATTERN.finditer(source):
        key = match.group("get_key") or match.group("bracket_key")
        if key:
            keys.add(key)
    return sorted(keys)


def enrich_pipeline_structurally(pipeline_name: str) -> PipelineMetadata:
    """Deterministic, regex-based extraction -- no model call, always available."""
    spec = PIPELINE_REGISTRY[pipeline_name]
    module_name = spec.etl_source_file.replace("/", ".").removesuffix(".py")
    module = __import__(module_name, fromlist=["_"])
    functions = {name: getattr(module, name) for name in spec.etl_function_names if hasattr(module, name)}
    combined_source = "\n".join(inspect.getsource(fn) for fn in functions.values())

    return PipelineMetadata(
        pipeline_name=pipeline_name,
        etl_source_file=spec.etl_source_file,
        functions=list(functions),
        source_datasets=list(spec.raw_tables),
        output_datasets=list(spec.curated_keys),
        joins=extract_joins(combined_source),
        filters=extract_filters(combined_source),
        business_rule_lookups=extract_business_rule_references(combined_source),
        test_files=[spec.test_file],
    )


_SUBMIT_TOOL_NAME = "submit_generated_context"

_SUBMIT_TOOL_SPEC = {
    "type": "function",
    "function": {
        "name": _SUBMIT_TOOL_NAME,
        "description": "Submit structured, technical context derived from reading this pipeline's ETL source.",
        "parameters": {
            "type": "object",
            "properties": {
                "grain": {"type": "string", "description": "e.g. 'portfolio-wide, one row per as_of_date'"},
                "caveats": {"type": "array", "items": {"type": "string"}},
                "derived_metrics": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "formula": {"type": "string"},
                            "source_fields": {"type": "array", "items": {"type": "string"}},
                        },
                        "required": ["name", "formula"],
                    },
                },
                "confidence": {
                    "type": "object",
                    "description": "field_name -> confidence (0-1) that this extraction is correct",
                    "additionalProperties": {"type": "number"},
                },
            },
            "required": ["grain", "derived_metrics"],
        },
    },
}

_SYSTEM_PROMPT = (
    "You read PySpark ETL source code and extract structured technical context: the "
    "computation's grain, any derived-metric formulas as readable expressions, and caveats "
    "about edge cases the code specifically guards against. You do not make business-policy "
    "judgments -- only describe what the code actually does. Call submit_generated_context "
    "exactly once with your findings."
)


def enrich_pipeline_with_codex(
    pipeline_name: str, model_client: DiagnosisModelClient, generated_at: str, source_commit: str | None = None
) -> GeneratedContext:
    """Structural extraction, plus a single model call for grain/caveats/derived-metric
    formulas the regex pass can't reliably produce. Raises ModelClientError on any model
    failure -- callers that want structural-only enrichment should call
    enrich_pipeline_structurally + GeneratedContext directly instead."""
    spec = PIPELINE_REGISTRY[pipeline_name]
    pipeline_metadata = enrich_pipeline_structurally(pipeline_name)

    module_name = spec.etl_source_file.replace("/", ".").removesuffix(".py")
    module = __import__(module_name, fromlist=["_"])
    functions = {name: getattr(module, name) for name in spec.etl_function_names if hasattr(module, name)}
    combined_source = "\n".join(inspect.getsource(fn) for fn in functions.values())

    messages = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user", "content": f"Pipeline: {pipeline_name}\n\nETL source:\n```python\n{combined_source}\n```"},
    ]
    response = model_client.send(messages, [_SUBMIT_TOOL_SPEC])
    submit_call = next((call for call in response.tool_calls if call.name == _SUBMIT_TOOL_NAME), None)
    if submit_call is None:
        raise ModelClientError(f"model did not call {_SUBMIT_TOOL_NAME!r}")

    args = submit_call.arguments
    return GeneratedContext(
        asset_id=pipeline_name,
        generated_by="codex",
        source_commit=source_commit,
        generated_at=generated_at,
        grain=args.get("grain"),
        sources=list(spec.raw_tables),
        joins=pipeline_metadata.joins,
        filters=pipeline_metadata.filters,
        derived_metrics=[
            {"name": m["name"], "formula": m["formula"], "source_fields": m.get("source_fields", [])}
            for m in args.get("derived_metrics", [])
        ],
        business_rule_references=pipeline_metadata.business_rule_lookups,
        caveats=args.get("caveats", []),
        confidence=args.get("confidence", {}),
        pipeline_metadata=pipeline_metadata,
    )
