"""
Tests for the packed CloudEvent formatter.
These tests do NOT need a running Drasi or Dapr instance.
"""
from drasi.reaction.models.ChangeEvent import ChangeEvent

from src.config import RouterQueryConfig
from src.formatter import format_packed

SAMPLE_CONFIG = RouterQueryConfig(
    pubsubName="agent-pubsub",
    topicName="support.sla-breach",
    skipControlSignals=True,
)

SAMPLE_CHANGE_EVENT_DICT = {
    "kind": "change",
    "queryId": "sla-breaches",
    "sequence": 42,
    "sourceTimeMs": 1700000000000,
    "addedResults": [{"ticket_id": "T001", "customer_id": "C123", "sla_hours": 25}],
    "updatedResults": [],
    "deletedResults": [],
}


def make_event() -> ChangeEvent:
    return ChangeEvent.model_validate(SAMPLE_CHANGE_EVENT_DICT)


def test_format_packed_is_single_dict():
    event = make_event()
    data = format_packed(event, SAMPLE_CONFIG)
    assert isinstance(data, dict)


def test_format_packed_fields():
    event = make_event()
    data = format_packed(event, SAMPLE_CONFIG)

    assert data["kind"] == "change"
    assert data["queryId"] == "sla-breaches"
    assert data["sequence"] == 42
    assert data["sourceTimeMs"] == 1700000000000


def test_format_packed_added_results():
    event = make_event()
    data = format_packed(event, SAMPLE_CONFIG)

    assert len(data["addedResults"]) == 1
    assert data["addedResults"][0]["ticket_id"] == "T001"
    assert data["updatedResults"] == []
    assert data["deletedResults"] == []


def test_format_packed_update():
    event_dict = {
        **SAMPLE_CHANGE_EVENT_DICT,
        "addedResults": [],
        "updatedResults": [
            {
                "before": {"ticket_id": "T001", "sla_hours": 23},
                "after": {"ticket_id": "T001", "sla_hours": 25},
            }
        ],
    }
    event = ChangeEvent.model_validate(event_dict)
    data = format_packed(event, SAMPLE_CONFIG)

    assert len(data["updatedResults"]) == 1
    assert data["updatedResults"][0]["before"]["sla_hours"] == 23
    assert data["updatedResults"][0]["after"]["sla_hours"] == 25


def test_format_packed_delete():
    event_dict = {
        **SAMPLE_CHANGE_EVENT_DICT,
        "addedResults": [],
        "deletedResults": [{"ticket_id": "T001"}],
    }
    event = ChangeEvent.model_validate(event_dict)
    data = format_packed(event, SAMPLE_CONFIG)

    assert len(data["deletedResults"]) == 1
    assert data["deletedResults"][0]["ticket_id"] == "T001"


def test_format_packed_mixed_operations():
    event_dict = {
        **SAMPLE_CHANGE_EVENT_DICT,
        "addedResults": [{"ticket_id": "T002"}],
        "updatedResults": [
            {"before": {"ticket_id": "T001", "sla_hours": 23}, "after": {"ticket_id": "T001", "sla_hours": 25}}
        ],
        "deletedResults": [{"ticket_id": "T000"}],
    }
    event = ChangeEvent.model_validate(event_dict)
    data = format_packed(event, SAMPLE_CONFIG)

    assert len(data["addedResults"]) == 1
    assert len(data["updatedResults"]) == 1
    assert len(data["deletedResults"]) == 1
