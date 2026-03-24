"""
Tests for DrasiAgentRouter change/control event handling.
Uses unittest.mock to avoid needing a real Dapr sidecar.
"""
import json
from unittest.mock import MagicMock, patch

import pytest
from drasi.reaction.models.ChangeEvent import ChangeEvent
from drasi.reaction.models.ControlEvent import ControlEvent

from src.router import DrasiAgentRouter


SAMPLE_CONFIG_DICT = {
    "pubsubName": "agent-pubsub",
    "topicName": "support.sla-breach",
    "skipControlSignals": True,
}

SAMPLE_CHANGE_DICT = {
    "kind": "change",
    "queryId": "sla-breaches",
    "sequence": 1,
    "sourceTimeMs": 1700000000000,
    "addedResults": [{"ticket_id": "T001"}],
    "updatedResults": [],
    "deletedResults": [],
}


@pytest.mark.asyncio
async def test_handle_change_publishes_packed_event():
    router = DrasiAgentRouter()
    event = ChangeEvent.model_validate(SAMPLE_CHANGE_DICT)

    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=None)

    with patch("src.router.DaprClient", return_value=mock_client):
        await router._handle_change(event, SAMPLE_CONFIG_DICT)

    mock_client.publish_event.assert_called_once()
    call_kwargs = mock_client.publish_event.call_args.kwargs

    assert call_kwargs["pubsub_name"] == "agent-pubsub"
    assert call_kwargs["topic_name"] == "support.sla-breach"

    published_data = json.loads(call_kwargs["data"])
    assert published_data["kind"] == "change"
    assert published_data["queryId"] == "sla-breaches"
    assert published_data["addedResults"][0]["ticket_id"] == "T001"

    metadata = call_kwargs["publish_metadata"]
    assert metadata["cloudevent.id"] == "sla-breaches-1"
    assert metadata["cloudevent.type"] == "drasi.change.packed"
    assert metadata["cloudevent.source"] == "drasi/query/sla-breaches"
    assert "cloudevent.time" in metadata


@pytest.mark.asyncio
async def test_handle_change_skips_none_config():
    router = DrasiAgentRouter()
    event = ChangeEvent.model_validate(SAMPLE_CHANGE_DICT)

    with patch("src.router.DaprClient") as mock_dapr:
        await router._handle_change(event, None)

    mock_dapr.assert_not_called()


@pytest.mark.asyncio
async def test_handle_control_skips_when_configured():
    router = DrasiAgentRouter()
    control_dict = {
        "kind": "control",
        "queryId": "sla-breaches",
        "sequence": 1,
        "sourceTimeMs": 1700000000000,
        "controlSignal": {"kind": "running"},
    }
    event = ControlEvent.model_validate(control_dict)

    with patch("src.router.DaprClient") as mock_dapr:
        await router._handle_control(event, SAMPLE_CONFIG_DICT)

    mock_dapr.assert_not_called()


@pytest.mark.asyncio
async def test_cloudevent_id_is_deterministic():
    """Same event published twice must produce the same cloudevent.id."""
    router = DrasiAgentRouter()
    event = ChangeEvent.model_validate(SAMPLE_CHANGE_DICT)

    ids = []
    for _ in range(2):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=None)

        with patch("src.router.DaprClient", return_value=mock_client):
            await router._handle_change(event, SAMPLE_CONFIG_DICT)

        ids.append(mock_client.publish_event.call_args.kwargs["publish_metadata"]["cloudevent.id"])

    assert ids[0] == ids[1] == "sla-breaches-1"


@pytest.mark.asyncio
async def test_oversized_payload_raises_before_publish():
    """Router must raise before calling publish_event if payload exceeds maxPayloadBytes."""
    router = DrasiAgentRouter()

    # Build an event whose serialized size will exceed the tiny limit we set below
    big_change = {
        **SAMPLE_CHANGE_DICT,
        "addedResults": [{"ticket_id": f"T{i}", "data": "x" * 500} for i in range(100)],
    }
    event = ChangeEvent.model_validate(big_change)

    # Set a 1-byte limit so any real payload triggers the guard
    tiny_limit_config = {**SAMPLE_CONFIG_DICT, "maxPayloadBytes": 1}

    with patch("src.router.DaprClient") as mock_dapr:
        with pytest.raises(ValueError, match="exceeds maxPayloadBytes"):
            await router._handle_change(event, tiny_limit_config)

    # Broker must never be reached — the check happens before DaprClient is opened
    mock_dapr.assert_not_called()


@pytest.mark.asyncio
async def test_payload_within_limit_publishes_normally():
    """Router must publish when payload is within maxPayloadBytes."""
    router = DrasiAgentRouter()
    event = ChangeEvent.model_validate(SAMPLE_CHANGE_DICT)

    # Set a generous limit that the small sample event will never hit
    large_limit_config = {**SAMPLE_CONFIG_DICT, "maxPayloadBytes": 10 * 1024 * 1024}

    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=None)

    with patch("src.router.DaprClient", return_value=mock_client):
        await router._handle_change(event, large_limit_config)

    mock_client.publish_event.assert_called_once()
