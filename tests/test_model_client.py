"""Tests for src/model_client.py's Responses-API translation layer
(OpenAIResponsesModelClient), which lets Codex-branded models (gpt-5-codex,
gpt-5.1-codex -- confirmed live to 404 on chat.completions, only available via
client.responses.create) serve any existing agent loop with zero changes to that loop.

No live API calls: _tools_to_responses_format/_messages_to_responses_input are pure
functions tested directly; send()'s response-parsing is tested by monkeypatching the
underlying openai SDK client's responses.create method, mirroring how this codebase
already prefers testing real logic over mocking third-party SDKs (no test file mocks
OpenAIDiagnosisModelClient's chat.completions.create either -- that class has no dedicated
unit tests at all, only ScriptedDiagnosisModelClient stands in for it everywhere).
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.model_client import (
    ModelClientError,
    OpenAIResponsesModelClient,
    ToolCall,
    _messages_to_responses_input,
    _tools_to_responses_format,
)

CHAT_COMPLETIONS_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_failed_checks",
            "description": "Return failing checks.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "compare_metric_definition_to_etl",
            "description": "Compare.",
            "parameters": {
                "type": "object",
                "properties": {"metric_name": {"type": "string"}},
                "required": ["metric_name"],
            },
        },
    },
]


def test_tools_to_responses_format_flattens_each_tool():
    result = _tools_to_responses_format(CHAT_COMPLETIONS_TOOLS)
    assert result == [
        {
            "type": "function",
            "name": "get_failed_checks",
            "description": "Return failing checks.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
        {
            "type": "function",
            "name": "compare_metric_definition_to_etl",
            "description": "Compare.",
            "parameters": {
                "type": "object",
                "properties": {"metric_name": {"type": "string"}},
                "required": ["metric_name"],
            },
        },
    ]


def test_tools_to_responses_format_defaults_missing_description_to_empty_string():
    tools = [{"type": "function", "function": {"name": "x", "parameters": {"type": "object", "properties": {}}}}]
    result = _tools_to_responses_format(tools)
    assert result[0]["description"] == ""


def test_messages_to_responses_input_passes_through_system_and_user():
    messages = [
        {"role": "system", "content": "You are a test."},
        {"role": "user", "content": "hello"},
    ]
    assert _messages_to_responses_input(messages) == [
        {"role": "system", "content": "You are a test."},
        {"role": "user", "content": "hello"},
    ]


def test_messages_to_responses_input_expands_multiple_tool_calls_in_one_assistant_message():
    messages = [
        {
            "role": "assistant",
            "tool_calls": [
                {"id": "call_1", "type": "function", "function": {"name": "tool_a", "arguments": "{}"}},
                {"id": "call_2", "type": "function", "function": {"name": "tool_b", "arguments": '{"x": 1}'}},
            ],
        }
    ]
    assert _messages_to_responses_input(messages) == [
        {"type": "function_call", "call_id": "call_1", "name": "tool_a", "arguments": "{}"},
        {"type": "function_call", "call_id": "call_2", "name": "tool_b", "arguments": '{"x": 1}'},
    ]


def test_messages_to_responses_input_converts_tool_role_to_function_call_output():
    messages = [{"role": "tool", "tool_call_id": "call_1", "content": '{"result": true}'}]
    assert _messages_to_responses_input(messages) == [
        {"type": "function_call_output", "call_id": "call_1", "output": '{"result": true}'}
    ]


def test_messages_to_responses_input_full_multi_turn_round_trip():
    messages = [
        {"role": "system", "content": "sys"},
        {"role": "user", "content": "starting_context"},
        {"role": "assistant", "tool_calls": [{"id": "call_1", "type": "function", "function": {"name": "get_failed_checks", "arguments": "{}"}}]},
        {"role": "tool", "tool_call_id": "call_1", "content": "{}"},
    ]
    result = _messages_to_responses_input(messages)
    assert result == [
        {"role": "system", "content": "sys"},
        {"role": "user", "content": "starting_context"},
        {"type": "function_call", "call_id": "call_1", "name": "get_failed_checks", "arguments": "{}"},
        {"type": "function_call_output", "call_id": "call_1", "output": "{}"},
    ]


def test_messages_to_responses_input_rejects_unrecognized_role():
    with pytest.raises(ModelClientError):
        _messages_to_responses_input([{"role": "developer", "content": "x"}])


# --- OpenAIResponsesModelClient.send() response parsing -----------------------------------


def _client_with_fake_responses_create(fake_create):
    client = OpenAIResponsesModelClient()
    client._client = SimpleNamespace(responses=SimpleNamespace(create=fake_create))
    return client


def test_send_extracts_function_call_items_and_ignores_reasoning_items():
    def fake_create(**kwargs):
        return SimpleNamespace(
            output=[
                SimpleNamespace(type="reasoning"),
                SimpleNamespace(type="function_call", call_id="call_1", name="get_failed_checks", arguments="{}"),
            ]
        )

    client = _client_with_fake_responses_create(fake_create)
    response = client.send([{"role": "user", "content": "go"}], CHAT_COMPLETIONS_TOOLS)
    assert response.tool_calls == [ToolCall(id="call_1", name="get_failed_checks", arguments={})]


def test_send_handles_multiple_function_calls_in_one_turn():
    def fake_create(**kwargs):
        return SimpleNamespace(
            output=[
                SimpleNamespace(type="function_call", call_id="call_1", name="tool_a", arguments="{}"),
                SimpleNamespace(type="function_call", call_id="call_2", name="tool_b", arguments='{"x": 1}'),
            ]
        )

    client = _client_with_fake_responses_create(fake_create)
    response = client.send([{"role": "user", "content": "go"}], CHAT_COMPLETIONS_TOOLS)
    assert response.tool_calls == [
        ToolCall(id="call_1", name="tool_a", arguments={}),
        ToolCall(id="call_2", name="tool_b", arguments={"x": 1}),
    ]


def test_send_raises_when_no_function_call_items_returned():
    def fake_create(**kwargs):
        return SimpleNamespace(output=[SimpleNamespace(type="reasoning")])

    client = _client_with_fake_responses_create(fake_create)
    with pytest.raises(ModelClientError):
        client.send([{"role": "user", "content": "go"}], CHAT_COMPLETIONS_TOOLS)


def test_send_raises_on_malformed_tool_call_arguments():
    def fake_create(**kwargs):
        return SimpleNamespace(
            output=[SimpleNamespace(type="function_call", call_id="call_1", name="tool_a", arguments="not json")]
        )

    client = _client_with_fake_responses_create(fake_create)
    with pytest.raises(ModelClientError):
        client.send([{"role": "user", "content": "go"}], CHAT_COMPLETIONS_TOOLS)


def test_send_wraps_sdk_exceptions_in_model_client_error():
    def fake_create(**kwargs):
        raise RuntimeError("boom")

    client = _client_with_fake_responses_create(fake_create)
    with pytest.raises(ModelClientError):
        client.send([{"role": "user", "content": "go"}], CHAT_COMPLETIONS_TOOLS)


def test_send_passes_translated_tools_and_input_to_responses_create():
    captured = {}

    def fake_create(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(output=[SimpleNamespace(type="function_call", call_id="c1", name="get_failed_checks", arguments="{}")])

    client = _client_with_fake_responses_create(fake_create)
    client.send([{"role": "user", "content": "go"}], CHAT_COMPLETIONS_TOOLS)

    assert captured["tool_choice"] == "required"
    assert captured["input"] == [{"role": "user", "content": "go"}]
    assert captured["tools"][0]["name"] == "get_failed_checks"
    assert "function" not in captured["tools"][0]  # flattened, not chat.completions-nested
