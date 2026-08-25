"""P0 reconciler canonical-exit ownership hardening.

This package shadows the legacy top-level ``ap_reconciler.py`` module and
changes only the recovery/reseed seams implicated by the 2026-08-25 NOW LIVE
incident.

Invariants:
* the reconciler may never redefine historical ``underlying_entry`` from a
  later/current market quote;
* when a canonical DB position appears after a temporary ``broker-repair-*``
  exit-engine owner, the reconciler must use the engine's canonical adoption
  API rather than generic ``add_position`` deduplication;
* exact client_id / execution_mode / contract identity is preserved;
* ambiguous or missing historical entry data stays explicitly untrusted and
  PR #176 continues to fail safe;
* no broker submit/cancel behavior is introduced.
"""

from __future__ import annotations

import importlib.util as _importlib_util
import json as _json
import sys as _sys
from datetime import datetime as _datetime, timezone as _timezone
from pathlib import Path as _Path
from typing import Any as _Any

_BASE_PATH = _Path(__file__).resolve().parent.parent / "ap_reconciler.py"
_BASE_MODULE_NAME = "_ap_reconciler_base"

_spec = _importlib_util.spec_from_file_location(_BASE_MODULE_NAME, _BASE_PATH)
if _spec is None or _spec.loader is None:  # pragma: no cover
    raise ImportError(f"Unable to load legacy ap_reconciler from {_BASE_PATH}")
_base = _importlib_util.module_from_spec(_spec)
_sys.modules[_BASE_MODULE_NAME] = _base
_spec.loader.exec_module(_base)

for _name in dir(_base):
    if not _name.startswith("__"):
        globals()[_name] = getattr(_base, _name)

_BaseAPBrokerReconciler = _base.APBrokerReconciler


def _positive_float(value: _Any) -> float:
    try:
        out = float(value)
        if out > 0 and out == out:
            return out
    except Exception:
        pass
    return 0.0


def _as_dict(value: _Any) -> dict:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            decoded = _json.loads(value)
            return dict(decoded) if isinstance(decoded, dict) else {}
        except Exception:
            return {}
    return {}


def _coerce_utc(value: _Any):
    if isinstance(value, _datetime):
        return value if value.tzinfo else value.replace(tzinfo=_timezone.utc)
    if isinstance(value, str) and value.strip():
        try:
            dt = _datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=_timezone.utc)
        except Exception:
            return None
    return None


def _immutable_underlying_from_entry_meta(meta: dict) -> tuple[float, str]:
    """Extract explicitly historical entry-underlying fields only.

    Generic ``current_underlying_price``, ``underlying_price``, trigger prices,
    and quote-repair values are intentionally excluded. They can be useful
    diagnostics, but their names/provenance do not prove that they represent the
    underlying at broker-confirmed fill time. Missing truth must remain missing.
    """
    candidates = (
        ("meta.underlying_entry", meta.get("underlying_entry")),
        ("meta.entry_underlying", meta.get("entry_underlying")),
        ("meta.underlying_price_at_entry", meta.get("underlying_price_at_entry")),
        ("meta.underlying_entry_price", meta.get("underlying_entry_price")),
        ("meta.entry_underlying_price", meta.get("entry_underlying_price")),
    )
    for source, raw in candidates:
        val = _positive_float(raw)
        if val > 0:
            return val, source
    return 0.0, ""


class APBrokerReconciler(_BaseAPBrokerReconciler):
    """Legacy reconciler with canonical identity adoption and immutable entry truth."""

    def _filled_entry_evidence(
        self,
        contract: str,
        *,
        position_id: str = "",
        position_entry_ts: _Any = None,
    ) -> dict | None:
        """Return one proven ENTRY fill without crossing lifecycle identity.

        Normal case: require the filled order to be linked to ``position_id``.

        Incident-race case: a canonical position may become visible a few seconds
        before ``orders.position_id`` is linked. If and only if the linked lookup
        misses, accept exactly one *unlinked* filled ENTRY for the same
        client/mode/contract whose fill timestamp is within five minutes of the
        canonical position entry timestamp. Multiple candidates, missing timing
        proof, or an order linked to another position all fail closed.
        """
        mode = _normalize_execution_mode(self.execution_mode)
        contract_u = self._norm_contract(contract)
        if mode is None or not contract_u or not self.client_id:
            return None

        columns = """
            SELECT position_id, local_order_id, broker_order_id,
                   signal_id, canonical_signal_id,
                   fill_price, filled_qty, filled_ts,
                   stop_underlying, target_underlying, meta
            FROM orders
            WHERE client_id = %s
              AND LOWER(TRIM(COALESCE(execution_mode, ''))) = %s
              AND UPPER(contract) = UPPER(%s)
              AND UPPER(COALESCE(kind, '')) = 'ENTRY'
              AND UPPER(COALESCE(status, '')) IN ('FILLED', 'PARTIAL_FILL')
              AND COALESCE(filled_qty, 0) > 0
        """

        try:
            from ap.db import conn, run_with_retry

            def _decorate(row: dict | None) -> dict | None:
                if not row:
                    return None
                out = dict(row)
                value, source = _immutable_underlying_from_entry_meta(
                    _as_dict(out.get("meta"))
                )
                out["underlying_entry"] = value
                out["underlying_entry_source"] = source
                return out

            if position_id:
                def _linked_query():
                    with conn() as c:
                        c.execute(
                            columns
                            + """
                              AND COALESCE(position_id, '') = %s
                            ORDER BY filled_ts DESC NULLS LAST,
                                     updated_ts DESC NULLS LAST,
                                     id DESC
                            LIMIT 1
                            """,
                            (self.client_id, mode, contract_u, position_id),
                        )
                        row = c.fetchone()
                        return dict(row) if row else None

                linked = run_with_retry(_linked_query)
                if linked:
                    return _decorate(linked)

                # The only safe fallback is the short position-link race. Do not
                # borrow an order already owned by a different canonical position.
                anchor_ts = _coerce_utc(position_entry_ts)
                if anchor_ts is None:
                    log.warning(
                        "[%s] FILLED_ENTRY_EVIDENCE_UNLINKED_BLOCKED contract=%s pos=%s "
                        "reason=missing_position_entry_ts",
                        self.client_id, contract_u, position_id,
                    )
                    return None

                def _unlinked_query():
                    with conn() as c:
                        c.execute(
                            columns
                            + """
                              AND COALESCE(position_id, '') = ''
                            ORDER BY filled_ts DESC NULLS LAST,
                                     updated_ts DESC NULLS LAST,
                                     id DESC
                            LIMIT 5
                            """,
                            (self.client_id, mode, contract_u),
                        )
                        return [dict(r) for r in (c.fetchall() or [])]

                unlinked = run_with_retry(_unlinked_query) or []
                candidates = []
                for row in unlinked:
                    filled_ts = _coerce_utc(row.get("filled_ts"))
                    if filled_ts is None:
                        continue
                    if abs((filled_ts - anchor_ts).total_seconds()) <= 300:
                        candidates.append(row)

                if len(candidates) == 1:
                    log.warning(
                        "[%s] FILLED_ENTRY_EVIDENCE_UNLINKED_RACE_RECOVERED "
                        "contract=%s pos=%s local_order=%s",
                        self.client_id,
                        contract_u,
                        position_id,
                        candidates[0].get("local_order_id") or "?",
                    )
                    return _decorate(candidates[0])
                if len(candidates) > 1:
                    log.critical(
                        "[%s] FILLED_ENTRY_EVIDENCE_UNLINKED_AMBIGUOUS "
                        "contract=%s pos=%s candidates=%d — refusing lifecycle cross-link",
                        self.client_id, contract_u, position_id, len(candidates),
                    )
                return None

            # No canonical position identity exists. Do not silently choose the
            # newest row when multiple historical ENTRY fills share a contract.
            def _unscoped_query():
                with conn() as c:
                    c.execute(
                        columns
                        + """
                        ORDER BY filled_ts DESC NULLS LAST,
                                 updated_ts DESC NULLS LAST,
                                 id DESC
                        LIMIT 2
                        """,
                        (self.client_id, mode, contract_u),
                    )
                    return [dict(r) for r in (c.fetchall() or [])]

            rows = run_with_retry(_unscoped_query) or []
            if len(rows) == 1:
                return _decorate(rows[0])
            if len(rows) > 1:
                log.critical(
                    "[%s] FILLED_ENTRY_EVIDENCE_AMBIGUOUS contract=%s mode=%s "
                    "candidates=%d — historical anchor remains untrusted",
                    self.client_id, contract_u, mode, len(rows),
                )
            return None
        except Exception as exc:
            log.warning(
                "[%s] FILLED_ENTRY_EVIDENCE_LOOKUP_FAILED contract=%s pos=%s mode=%s error=%s",
                self.client_id, contract_u, position_id or "n/a", mode, exc,
            )
            return None

    def _derive_underlying_entry_from_position(
        self,
        pos: dict,
        *,
        underlying: str,
        contract: str,
    ) -> float:
        """Return historical entry truth only; never substitute the current market."""
        for key in (
            "underlying_entry",
            "entry_underlying",
            "underlying_entry_price",
            "entry_underlying_price",
        ):
            val = self._safe_float(pos.get(key), 0.0)
            if val > 0:
                return val

        position_id = str(pos.get("id") or pos.get("position_id") or "").strip()
        evidence = self._filled_entry_evidence(
            contract,
            position_id=position_id,
            position_entry_ts=pos.get("entry_ts"),
        ) or {}
        return _positive_float(evidence.get("underlying_entry"))

    def _derive_underlying_entry_from_broker_position(
        self,
        bp: dict,
        *,
        underlying: str,
        contract: str,
    ) -> float:
        """Broker imports accept explicit historical fields, not current-market aliases."""
        for key in (
            "underlying_entry", "underlying_entry_price", "entry_underlying",
            "underlying_price_at_entry",
        ):
            val = self._safe_float(bp.get(key), 0.0)
            if val > 0:
                return val

        evidence = self._filled_entry_evidence(contract) or {}
        return _positive_float(evidence.get("underlying_entry"))

    def _seed_exit_engine_from_position(self, pos: dict) -> None:
        """Seed/adopt an existing canonical DB position with its durable identity."""
        if not pos:
            return
        pos_id = str(pos.get("id") or pos.get("position_id") or "").strip()
        contract = self._norm_contract(pos.get("contract") or pos.get("symbol") or "")
        underlying = self._norm_underlying(
            pos.get("underlying") or pos.get("ticker") or self._norm_underlying(contract)
        )
        side = str(pos.get("direction") or pos.get("side") or "CALL").upper()
        qty = int(
            pos.get("quantity_remaining")
            or pos.get("qty")
            or pos.get("quantity")
            or 0
        )
        entry_px = self._safe_float(pos.get("avg_fill") or pos.get("entry_price"), 0.0)
        if qty <= 0 or entry_px <= 0 or not contract or not pos_id:
            return

        position_entry_ts = pos.get("entry_ts")
        evidence = self._filled_entry_evidence(
            contract,
            position_id=pos_id,
            position_entry_ts=position_entry_ts,
        ) or {}
        underlying_entry = self._derive_underlying_entry_from_position(
            pos, underlying=underlying, contract=contract
        )
        stop_underlying = (
            pos.get("stop_underlying")
            or pos.get("underlying_stop")
            or evidence.get("stop_underlying")
            or 0.0
        )
        target_underlying = (
            pos.get("target_underlying")
            or pos.get("underlying_target")
            or evidence.get("target_underlying")
            or 0.0
        )
        price_untrusted = bool(
            pos.get("price_untrusted")
            or str(pos.get("close_confidence") or "").upper().endswith("PRICE_UNTRUSTED")
        )

        self._seed_exit_engine_from_import(
            pos_id=pos_id,
            contract=contract,
            underlying=underlying,
            side=side,
            qty=qty,
            entry_px=entry_px,
            stop_underlying=stop_underlying,
            target_underlying=target_underlying,
            underlying_entry=underlying_entry,
            price_untrusted=price_untrusted,
            entry_evidence=evidence,
            position_entry_ts=position_entry_ts,
        )

    def _seed_exit_engine_from_import(
        self,
        *,
        pos_id: str,
        contract: str,
        underlying: str,
        side: str,
        qty: int,
        entry_px: float,
        stop_underlying=0.0,
        target_underlying=0.0,
        underlying_entry: float = 0.0,
        price_untrusted: bool = False,
        entry_evidence: dict | None = None,
        position_entry_ts: _Any = None,
    ) -> None:
        """Adopt a broker-repair owner into canonical identity before generic add.

        Crucially, missing ``underlying_entry`` remains zero/untrusted. This
        allows existing degraded-data guards to HOLD rather than rewriting
        history from the current market and pretending the data is trustworthy.
        """
        ee = getattr(self, "exit_engine", None)
        if not ee:
            return super()._seed_exit_engine_from_import(
                pos_id=pos_id,
                contract=contract,
                underlying=underlying,
                side=side,
                qty=qty,
                entry_px=entry_px,
                stop_underlying=stop_underlying,
                target_underlying=target_underlying,
                underlying_entry=underlying_entry,
                price_untrusted=price_untrusted,
            )

        try:
            from ap_exit_engine import ManagedPosition

            mode = _normalize_execution_mode(self.execution_mode)
            if mode is None:
                log.critical(
                    "[%s] RECONCILER_CANONICAL_ADOPTION_BLOCKED mode_unproven contract=%s pos=%s",
                    self.client_id, contract, pos_id,
                )
                return

            evidence = dict(
                entry_evidence
                or self._filled_entry_evidence(
                    contract,
                    position_id=pos_id,
                    position_entry_ts=position_entry_ts,
                )
                or {}
            )
            persisted_underlying = _positive_float(underlying_entry)
            if persisted_underlying <= 0:
                persisted_underlying = _positive_float(evidence.get("underlying_entry"))

            stop_u = self._safe_float(
                stop_underlying or evidence.get("stop_underlying"), 0.0
            )
            target_u = self._safe_float(
                target_underlying or evidence.get("target_underlying"), 0.0
            )
            local_order_id = str(evidence.get("local_order_id") or "").strip()
            broker_order_id = str(evidence.get("broker_order_id") or "").strip()
            signal_id = str(evidence.get("signal_id") or f"reconciled:{contract}").strip()
            canonical_signal_id = str(
                evidence.get("canonical_signal_id") or evidence.get("signal_id") or signal_id
            ).strip()
            entry_ts = evidence.get("filled_ts") or position_entry_ts

            adopt = getattr(ee, "adopt_canonical_position_identity", None)
            if callable(adopt):
                result = adopt(
                    contract=contract,
                    canonical_position_id=str(pos_id or ""),
                    local_order_id=local_order_id,
                    broker_order_id=broker_order_id,
                    signal_id=signal_id,
                    canonical_signal_id=canonical_signal_id,
                    entry_fill=float(entry_px),
                    entry_ts=entry_ts,
                    order_filled_ts=evidence.get("filled_ts"),
                    execution_mode=mode,
                    client_id=self.client_id,
                    underlying_entry=float(persisted_underlying),
                    score=0.0,
                    tier="RECONCILED",
                    pattern="",
                    direction=side,
                    timeframe="",
                    underlying_stop=float(stop_u),
                    underlying_target=float(target_u),
                )
                disposition = str(getattr(result, "disposition", "") or "")
                adopted = bool(getattr(result, "adopted", False))
                retryable = bool(getattr(result, "retryable", False))
                safe_to_seed = bool(getattr(result, "safe_to_seed", False))

                if adopted or disposition in {"ADOPTED", "ALREADY_CANONICAL_REPAIR_REMOVED"}:
                    log.critical(
                        "[%s] EXIT_ENGINE_CANONICAL_ADOPTED_FROM_RECONCILER contract=%s pos=%s disposition=%s underlying_entry=%s source=%s",
                        self.client_id, contract, pos_id, disposition or "ADOPTED",
                        persisted_underlying if persisted_underlying > 0 else "unknown",
                        evidence.get("underlying_entry_source") or "db_position",
                    )
                    return
                if disposition.startswith("RETRY_") or (retryable and not safe_to_seed):
                    log.critical(
                        "[%s] EXIT_ENGINE_CANONICAL_ADOPTION_RETRY_HOLD contract=%s pos=%s disposition=%s — refusing second owner",
                        self.client_id, contract, pos_id, disposition or "RETRY_UNKNOWN",
                    )
                    return
                if disposition and disposition != "NO_REPAIR_FOUND" and not safe_to_seed:
                    log.critical(
                        "[%s] EXIT_ENGINE_CANONICAL_ADOPTION_HOLD contract=%s pos=%s disposition=%s",
                        self.client_id, contract, pos_id, disposition,
                    )
                    return

            # No repair exists. Seed exactly one canonical ManagedPosition.
            mp = ManagedPosition(
                ticker=self._norm_underlying(underlying or contract),
                option_symbol=contract,
                side=side,
                quantity=int(qty),
                entry_price=float(entry_px),
                underlying_entry=float(persisted_underlying),
                underlying_target=float(target_u),
                underlying_stop=float(stop_u),
            )
            mp.position_id = str(pos_id or "")
            mp.client_id = self.client_id
            mp.signal_id = signal_id
            mp.canonical_signal_id = canonical_signal_id
            mp.execution_mode = mode
            mp.current_option_price = float(entry_px)
            mp.price_untrusted = bool(price_untrusted)
            mp.underlying_entry_untrusted = persisted_underlying <= 0
            mp.imported_by_reconciler = True
            if local_order_id:
                mp.entry_local_order_id = local_order_id
            if broker_order_id:
                mp.entry_broker_order_id = broker_order_id
            ee.add_position(mp)
            log.critical(
                "[%s] EXIT_ENGINE_SEEDED_CANONICAL_FROM_RECONCILER | %s | qty=%s entry=%.4f underlying_entry=%s untrusted=%s pos=%s",
                self.client_id, contract, qty, entry_px,
                persisted_underlying if persisted_underlying > 0 else "unknown",
                persisted_underlying <= 0, pos_id or "n/a",
            )
        except Exception as exc:
            log.error(
                "[%s] Failed canonical reconciler seed for %s: %s",
                self.client_id, contract, exc, exc_info=True,
            )
            try:
                self._record_reconciler_rejection(
                    signal_id=str(pos_id or f"reconciled:{contract}"),
                    ticker=self._norm_underlying(underlying or contract),
                    category_name="EXECUTION",
                    severity_name="CRITICAL",
                    reason_code="EXIT_ENGINE_CANONICAL_ADOPTION_FAILED",
                    human_reason=f"canonical exit owner adoption failed: {exc}",
                    contract=contract,
                    pos_id=pos_id,
                    qty=qty,
                    side=side,
                    entry_px=entry_px,
                    underlying_entry=underlying_entry,
                    price_untrusted=price_untrusted,
                )
            except Exception:
                pass


# Harden standard import surface.
globals()["APBrokerReconciler"] = APBrokerReconciler
