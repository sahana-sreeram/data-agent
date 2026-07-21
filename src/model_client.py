"""Model-client abstraction so diagnosis logic isn't coupled to one API call.

DiagnosisModelClient is a Protocol with a single method: send messages and
tool specs, get back the tool calls the model wants to make this turn.
OpenAIDiagnosisModelClient is the only implementation that calls a live API;
ScriptedDiagnosisModelClient is a test double that returns a pre-programmed
sequence of responses. Tests use only the latter -- see
tests/test_diagnosis_agent.py and tests/test_diagnose_incident.py.

OPENAI_API_KEY is read from the environment (loaded here from a .env file at
the project root, if present, via python-dotenv) -- never hardcoded, never
logged, never sent anywhere but directly to the openai SDK.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Protocol

from dotenv import load_dotenv

load_dotenv()

DEFAULT_MODEL = "gpt-5"
# None means "don't send temperature at all". Newer reasoning models (gpt-5
# and friends) only support their default temperature and reject any
# explicit value, including 0.0, with a 400 error -- so we omit the
# parameter entirely unless the caller explicitly overrides it for a model
# that does support tuning it.
DEFAULT_TEMPERATURE = None
DEFAULT_TIMEOUT_SECONDS = 60.0


class ModelClientError(Exception):
    """Raised for API failures, timeouts, missing credentials, or malformed model responses."""


@dataclass(frozen=True)
class ToolCall:
    id: str
    name: str
    arguments: dict


@dataclass(frozen=True)
class ModelResponse:
    tool_calls: list[ToolCall]


class DiagnosisModelClient(Protocol):
    def send(self, messages: list[dict], tools: list[dict]) -> ModelResponse: ...


class OpenAIDiagnosisModelClient:
    """Calls OpenAI's chat completions API with tool calling, forcing a tool call every turn."""

    def __init__(
        self,
        model: str = DEFAULT_MODEL,
        temperature: float | None = DEFAULT_TEMPERATURE,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        try:
            import openai
        except ImportError as exc:
            raise ModelClientError("the 'openai' package is required to use OpenAIDiagnosisModelClient") from exc

        if not os.environ.get("OPENAI_API_KEY"):
            raise ModelClientError(
                "OPENAI_API_KEY is not set. Copy .env.example to .env at the project root and fill in "
                "your key, or export OPENAI_API_KEY in your shell."
            )

        self._model = model
        self._temperature = temperature
        self._client = openai.OpenAI(timeout=timeout)

    def send(self, messages: list[dict], tools: list[dict]) -> ModelResponse:
        kwargs = {"model": self._model, "messages": messages, "tools": tools, "tool_choice": "required"}
        if self._temperature is not None:
            kwargs["temperature"] = self._temperature

        try:
            response = self._client.chat.completions.create(**kwargs)
        except Exception as exc:  # noqa: BLE001 -- any SDK/timeout/API error becomes a controlled failure
            raise ModelClientError(f"model request failed: {exc}") from exc

        choice = response.choices[0]
        raw_tool_calls = choice.message.tool_calls or []
        if not raw_tool_calls:
            raise ModelClientError("model response contained no tool calls (tool_choice='required' was not honored)")

        tool_calls = []
        for call in raw_tool_calls:
            try:
                arguments = json.loads(call.function.arguments)
            except (json.JSONDecodeError, TypeError) as exc:
                raise ModelClientError(f"model returned malformed tool-call arguments: {exc}") from exc
            tool_calls.append(ToolCall(id=call.id, name=call.function.name, arguments=arguments))

        return ModelResponse(tool_calls=tool_calls)


class ScriptedDiagnosisModelClient:
    """Test double: returns a pre-programmed sequence of ModelResponse objects, one per .send() call.

    Never imports or touches the openai package, so it carries no live-API
    dependency at all.
    """

    def __init__(self, responses: list[ModelResponse]) -> None:
        self._responses = list(responses)
        self._call_count = 0

    def send(self, messages: list[dict], tools: list[dict]) -> ModelResponse:
        if self._call_count >= len(self._responses):
            raise ModelClientError("ScriptedDiagnosisModelClient exhausted its scripted responses")
        response = self._responses[self._call_count]
        self._call_count += 1
        return response
