"""Kafka consumers for claims-status-service.

One handler per subscribed topic. Handlers are best-effort logging plus
audit — services override this file to implement real cross-domain behavior.
"""
from __future__ import annotations

import logging

from healthcare_common.audit import emit_audit

log = logging.getLogger("claims-status-service.consumers")


def register(svc) -> None:
    bus = svc.bus

    @bus.on("claim.submitted")
    def _on_claim_submitted(envelope: dict) -> None:
        log.info("claims-status-service: received claim.submitted id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.claim.submitted", actor="system:claims-status-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("claim.adjudicated")
    def _on_claim_adjudicated(envelope: dict) -> None:
        log.info("claims-status-service: received claim.adjudicated id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.claim.adjudicated", actor="system:claims-status-service",
                   target=None, details={"envelope_id": envelope.get("id")})

