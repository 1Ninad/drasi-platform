"""
DrasiAgentRouter: The core of the Router Reaction.

Composes DrasiReaction (from the Drasi Python SDK) to:
1. Subscribe to Drasi query result topics (drasi-pubsub: {queryId}-results)
2. Transform each ChangeEvent into a single packed CloudEvent
3. Publish that CloudEvent to the agent-accessible Dapr Pub/Sub topic

Each Drasi ChangeEvent becomes exactly one CloudEvent:
  - id:     {queryId}-{sequence}   (deterministic — enables deduplication on re-delivery)
  - type:   drasi.change.packed
  - source: drasi/query/{queryId}
  - time:   ISO 8601 from sourceTimeMs
  - data:   full ChangeEvent payload (addedResults, updatedResults, deletedResults)
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from dapr.clients import DaprClient
from drasi.reaction.models.ChangeEvent import ChangeEvent
from drasi.reaction.models.ControlEvent import ControlEvent
from drasi.reaction.sdk import DrasiReaction

from .config import RouterQueryConfig, parse_query_config
from .formatter import format_packed

logger = logging.getLogger(__name__)


class DrasiAgentRouter:
    """
    Routes Drasi query change events to Dapr Pub/Sub topics for AI agents.

    How it works:
    1. DrasiReaction subscribes to {queryId}-results topics on drasi-pubsub
    2. On each ChangeEvent, reads the per-query config (which pubsub/topic to publish to)
    3. Formats the event as a packed CloudEvent and publishes it to the agent topic
    4. Agents subscribed to that topic wake up and process the change

    Example:
        router = DrasiAgentRouter()
        router.start()  # Blocks, processing events
    """

    def __init__(self, port: int = 80):
        self._port = port
        self._reaction = DrasiReaction(
            on_change_event=self._handle_change,
            on_control_event=self._handle_control,
            parse_query_configs=parse_query_config,
            port=port,
        )
        # Populated as events arrive; shared with the MCP server for query discovery
        self._query_configs: dict[str, RouterQueryConfig] = {}

    def start(self):
        """Start the router. Blocks indefinitely, processing events."""
        logger.info("Starting Drasi Agent Router")
        self._reaction.start()

    async def _handle_change(self, event: ChangeEvent, raw_config: Any) -> None:
        """
        Called by DrasiReaction when a change event arrives from Drasi.

        Validates the per-query config, formats the event as a packed CloudEvent,
        and publishes it to the agent's Dapr Pub/Sub topic.
        """
        if raw_config is None:
            logger.warning(
                "No config found for query '%s' - skipping. "
                "Check /etc/queries/%s exists with topicName set.",
                event.queryId, event.queryId,
            )
            return

        try:
            config = RouterQueryConfig.model_validate(raw_config)
        except Exception as e:
            logger.error("Invalid config for query '%s': %s", event.queryId, e)
            return

        # Cache config so the MCP server can expose it for agent discovery
        self._query_configs[event.queryId] = config

        logger.info(
            "Routing change event: query=%s seq=%d -> %s/%s",
            event.queryId, event.sequence, config.pubsub_name, config.topic_name,
        )

        data = format_packed(event, config)
        ce_id = f"{event.queryId}-{event.sequence}"
        ce_time = datetime.fromtimestamp(
            event.sourceTimeMs / 1000.0, tz=timezone.utc
        ).isoformat()

        # Serialize here so we know the exact wire size before touching the broker.
        # We check bytes, not the dict, because JSON encoding adds overhead (escaping,
        # unicode, etc.) that makes the actual payload larger than the raw Python object.
        data_bytes = json.dumps(data).encode("utf-8")

        if len(data_bytes) > config.max_payload_bytes:
            # Raising here causes Dapr to retry, which will hit the same check every time.
            # That's intentional — it gives the operator consistent, clear failures in logs
            # rather than a silent drop or an opaque broker error buried in Dapr's output.
            # The fix is always upstream: narrow the Drasi query scope so batches are smaller,
            # or raise maxPayloadBytes if your broker supports larger messages (e.g. Kafka,
            # Azure Service Bus Premium, RabbitMQ).
            raise ValueError(
                f"Packed CloudEvent for query '{event.queryId}' seq={event.sequence} "
                f"is {len(data_bytes):,} bytes, exceeds maxPayloadBytes={config.max_payload_bytes:,}. "
                f"Narrow the Drasi query scope or raise maxPayloadBytes in the query config."
            )

        with DaprClient() as client:
            try:
                client.publish_event(
                    pubsub_name=config.pubsub_name,
                    topic_name=config.topic_name,
                    data=data_bytes.decode("utf-8"),
                    data_content_type="application/json",
                    publish_metadata={
                        "cloudevent.id": ce_id,
                        "cloudevent.type": "drasi.change.packed",
                        "cloudevent.source": f"drasi/query/{event.queryId}",
                        "cloudevent.time": ce_time,
                    },
                )
                logger.debug(
                    "Published CloudEvent id=%s (%d bytes) to %s/%s",
                    ce_id, len(data_bytes), config.pubsub_name, config.topic_name,
                )
            except Exception as e:
                logger.error(
                    "Failed to publish event for query '%s' to %s/%s: %s",
                    event.queryId, config.pubsub_name, config.topic_name, e,
                )
                raise  # Re-raise so Dapr retries delivery

    async def _handle_control(self, event: ControlEvent, raw_config: Any) -> None:
        """
        Called by DrasiReaction when a control signal arrives (e.g., query started/stopped).

        Skipped by default (skip_control_signals=True). Set to False in the query config
        if agents need to react to query lifecycle events.
        """
        if raw_config is None:
            return

        try:
            config = RouterQueryConfig.model_validate(raw_config)
        except Exception:
            return

        if config.skip_control_signals:
            logger.debug(
                "Skipping control signal for query '%s' (skip_control_signals=True)",
                event.queryId,
            )
            return

        signal_kind = getattr(event.controlSignal, "kind", "unknown")
        logger.info(
            "Routing control signal: query=%s signal=%s -> %s/%s",
            event.queryId, signal_kind, config.pubsub_name, config.topic_name,
        )

        payload = {
            "kind": "control",
            "queryId": event.queryId,
            "sequence": event.sequence,
            "sourceTimeMs": event.sourceTimeMs,
            "controlSignal": {"kind": signal_kind},
        }
        ce_time = datetime.fromtimestamp(
            event.sourceTimeMs / 1000.0, tz=timezone.utc
        ).isoformat()

        with DaprClient() as client:
            client.publish_event(
                pubsub_name=config.pubsub_name,
                topic_name=config.topic_name,
                data=json.dumps(payload),
                data_content_type="application/json",
                publish_metadata={
                    "cloudevent.id": f"{event.queryId}-{event.sequence}",
                    "cloudevent.type": f"drasi.control.{signal_kind}",
                    "cloudevent.source": f"drasi/query/{event.queryId}",
                    "cloudevent.time": ce_time,
                },
            )
