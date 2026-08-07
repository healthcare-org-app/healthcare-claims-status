"""Kafka consumers for claims-status-service.

One handler per subscribed topic. Real handlers write to this service's own
database and/or publish follow-up events; stub handlers just log + audit.
"""
from __future__ import annotations

import logging

from psycopg.types.json import Json

from healthcare_common.audit import emit_audit

log = logging.getLogger("claims-status-service.consumers")

TABLE = "claims_status"


def register(svc) -> None:
    bus = svc.bus
    db = svc.db
    clients = svc.clients

    @bus.on("claim.submitted")
    def _on_claim_submitted(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    db.execute(f"INSERT INTO {TABLE} (data) VALUES (%s)",
                               (Json({"claim_id": data.get("id"), "state": "submitted"}),))
        except Exception as e:
            log.exception("claims-status-service/claim.submitted handler failed: %s", e)
        emit_audit(bus, action="consume.claim.submitted", actor="system:claims-status-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("claim.adjudicated")
    def _on_claim_adjudicated(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    cid = data.get("claim_id") or data.get("id")
                    db.execute(f"UPDATE {TABLE} SET data = data || %s, updated_at=now() "
                               f"WHERE data->>'claim_id' = %s",
                               (Json({"state": data.get("status", "adjudicated")}), str(cid)))
        except Exception as e:
            log.exception("claims-status-service/claim.adjudicated handler failed: %s", e)
        emit_audit(bus, action="consume.claim.adjudicated", actor="system:claims-status-service",
                   target=None, details={"envelope_id": envelope.get("id")})

