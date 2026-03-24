"""
Per-query configuration for the Drasi-to-Dapr-Agents router.

Each Drasi query that this reaction subscribes to has its own config file
at /etc/queries/{queryId}. This module parses those YAML files.

Example YAML config for a query:
    pubsubName: agent-pubsub
    topicName: support.sla-breach
    skipControlSignals: true
    maxPayloadBytes: 204800
"""
from __future__ import annotations

import os
from io import TextIOWrapper

import yaml
from pydantic import BaseModel, Field

# 200 KB — safely below the tightest hard broker limits (AWS SQS and Azure Service Bus
# Standard are both fixed at 256 KB and cannot be raised). The 56 KB headroom accounts
# for CloudEvent envelope fields and JSON encoding overhead.
_DEFAULT_MAX_PAYLOAD_BYTES = 200 * 1024


class RouterQueryConfig(BaseModel):
    """
    Configuration for routing one Drasi query's results to a Dapr Pub/Sub topic.

    This is loaded from /etc/queries/{queryId} YAML files.
    """
    pubsub_name: str = Field(
        default_factory=lambda: os.getenv("DefaultPubsubName", "agent-pubsub"),
        alias="pubsubName",
        description="Dapr pub/sub component name to publish to (for agents)",
    )
    topic_name: str = Field(
        alias="topicName",
        description="Dapr pub/sub topic to publish to (agents subscribe to this)",
    )
    skip_control_signals: bool = Field(
        default=True,
        alias="skipControlSignals",
        description="If True, control signals (bootstrap, running, stopped) are not forwarded",
    )
    max_payload_bytes: int = Field(
        default=_DEFAULT_MAX_PAYLOAD_BYTES,
        alias="maxPayloadBytes",
        description=(
            "Maximum serialized payload size in bytes before the router refuses to publish. "
            "Set this to match your broker's limit. "
            "Defaults to 200 KB (safe floor for SQS and Azure Service Bus Standard)."
        ),
    )

    model_config = {"populate_by_name": True}


def parse_query_config(file: TextIOWrapper) -> dict:
    """
    Parse a query config YAML file into a dict.
    Used as the parse_query_configs callback for DrasiReaction.
    """
    return yaml.safe_load(file) or {}
