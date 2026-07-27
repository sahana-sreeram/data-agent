"""Validation gate: enrichment output must pass schema validation before it's ever written
through a ContextStore. Never lets a malformed or partially-valid result through -- reject and
report, don't coerce."""

from __future__ import annotations

from pydantic import BaseModel, ValidationError


class EnrichmentValidationError(Exception):
    """Raised when model-generated or code-derived context fails schema validation."""


def validate_context(model_cls: type[BaseModel], raw: dict) -> BaseModel:
    """Validate `raw` against `model_cls`. Raises EnrichmentValidationError (with the
    underlying Pydantic error messages) rather than returning a partially-valid object."""
    try:
        return model_cls.model_validate(raw)
    except ValidationError as exc:
        raise EnrichmentValidationError(f"{model_cls.__name__} failed validation: {exc}") from exc
