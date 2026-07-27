"""Guards that context/human/*.yaml stay valid against the real HumanAnnotation schema --
these are hand-authored, so nothing else exercises them automatically."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from src.context_store.models import HumanAnnotation

HUMAN_ANNOTATION_FILES = sorted(Path("context/human").glob("*.yaml"))


def test_at_least_one_human_annotation_exists():
    assert HUMAN_ANNOTATION_FILES


@pytest.mark.parametrize("path", HUMAN_ANNOTATION_FILES, ids=lambda p: p.stem)
def test_human_annotation_validates_against_the_real_schema(path):
    raw = yaml.safe_load(path.read_text())
    annotation = HumanAnnotation.model_validate(raw)
    assert annotation.data_product == path.stem


@pytest.mark.parametrize("path", HUMAN_ANNOTATION_FILES, ids=lambda p: p.stem)
def test_human_annotation_targets_a_real_registered_pipeline(path):
    from src.lifecycle_pipeline_registry import PIPELINE_REGISTRY

    assert path.stem in PIPELINE_REGISTRY
