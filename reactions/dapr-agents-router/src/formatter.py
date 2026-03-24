"""
Transforms a Drasi ChangeEvent into a single packed CloudEvent data payload.

One ChangeEvent → one CloudEvent. The entire batch (all adds, updates, deletes)
goes into the `data` field. The CloudEvent envelope fields (id, type, source, time)
are set by the router at publish time.
"""
from __future__ import annotations

from typing import Any

from drasi.reaction.models.ChangeEvent import ChangeEvent

from .config import RouterQueryConfig


def format_packed(event: ChangeEvent, config: RouterQueryConfig) -> dict[str, Any]:
    """
    Return the CloudEvent `data` payload for a ChangeEvent.

    Contains all addedResults, updatedResults, and deletedResults from the
    original Drasi batch, plus queryId and sequence for agent-side deduplication.
    """
    return {
        "kind": "change",
        "queryId": event.queryId,
        "sequence": event.sequence,
        "sourceTimeMs": event.sourceTimeMs,
        "addedResults": [r.root for r in event.addedResults],
        "updatedResults": [
            {
                "before": u.before.root if u.before else None,
                "after": u.after.root if u.after else None,
            }
            for u in event.updatedResults
        ],
        "deletedResults": [r.root for r in event.deletedResults],
        "metadata": event.metadata.root if event.metadata else None,
    }
