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
import math as _math
import re as _re
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


_OCC_CP_RE = _re.compile(r"^[A-Z0-9]{1,6}(\d{6})(C|P)(\d{8})$")


def _strict_option_side(contract: str, persisted: _Any) -> "str | None":
    """Return 'CALL' or 'PUT' with OCC-contract proof; fail closed on every ambiguity.

    Policy:
    1. Parse C/P from OCC contract marker — only deterministic source.
    2. If persisted present: normalize to CALL/PUT; accepted tokens: 'CALL','PUT','C','P'.
       Anything else → None (fail closed).
    3. If normalized persisted conflicts with OCC → None (fail closed).
    4. If persisted absent → derive from OCC directly.
    5. Never default to CALL. Never default to PUT.
    """
    contract_u = (contract or "").strip().upper()
    m = _OCC_CP_RE.match(contract_u)
    if not m:
        return None
    occ_side = "CALL" if m.group(2) == "C" else "PUT"
    raw = str(persisted).strip().upper() if persisted is not None else ""
    if not raw:
        return occ_side
    if raw in {"CALL", "C"}:
        persisted_side = "CALL"
    elif raw in {"PUT", "P"}:
        persisted_side = "PUT"
    else:
        return None
    if persisted_side != occ_side:
        return None
    return occ_side


def _positive_float(value: _Any) -> float:
    if isinstance(value, bool):
        return 0.0
    try:
        out = float(value)
        if _math.isfinite(out) and out > 0:
            return out
    except Exception:
        pass
    return 0.0


def _positive_integral(value: _Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        out = float(value)
        if not _math.isfinite(out) or out <= 0 or out != int(out):
            return None
        return int(out)
    except (TypeError, ValueError, OverflowError):
        return None


def _proven_broker_order_id(value: _Any) -> bool:
    if isinstance(value, bool):
        return False
    token = str(value or "").strip().upper()
    if token in {
        "", "0", "N/A", "NA", "NONE", "NULL", "UNKNOWN", "UNDEFINED",
        "NIL", "TRUE", "FALSE", "NAN", "INF", "+INF", "-INF",
    }:
        return False
    try:
        numeric = float(token)
        if not _math.isfinite(numeric) or numeric <= 0:
            return False
    except (TypeError, ValueError, OverflowError):
        pass
    return True


def _strict_execution_mode(value: _Any) -> str | None:
    """Accept only the canonical persisted mode tokens; never infer a mode."""
    if not isinstance(value, str):
        return None
    return value if value in {"live", "paper"} else None


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
        mode = _strict_execution_mode(self.execution_mode)
        contract_u = self._norm_contract(contract)
        if mode is None or not contract_u or not str(self.client_id or "").strip():
            return None

        columns = """
            SELECT position_id, local_order_id, broker_order_id,
                   signal_id, canonical_signal_id,
                   fill_price, filled_qty, filled_ts,
                   stop_underlying, target_underlying, meta
            FROM orders
            WHERE client_id = %s
              AND execution_mode = %s
              AND contract = %s
              AND kind = 'ENTRY'
              AND status IN ('FILLED', 'PARTIAL_FILL')
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

            def _validated_rows(rows: list[dict] | None) -> list[dict] | None:
                """Validate every exact-identity row before selecting any candidate."""
                validated = []
                for raw_row in rows or []:
                    row = dict(raw_row)
                    if not _proven_broker_order_id(row.get("broker_order_id")):
                        log.critical(
                            "[%s] FILLED_ENTRY_EVIDENCE_BROKER_ID_UNPROVEN "
                            "contract=%s mode=%s — refusing historical donation",
                            self.client_id, contract_u, mode,
                        )
                        return None
                    if _positive_float(row.get("fill_price")) <= 0:
                        log.critical(
                            "[%s] FILLED_ENTRY_EVIDENCE_FILL_PRICE_UNPROVEN "
                            "contract=%s mode=%s — refusing historical donation",
                            self.client_id, contract_u, mode,
                        )
                        return None
                    if _positive_integral(row.get("filled_qty")) is None:
                        log.critical(
                            "[%s] FILLED_ENTRY_EVIDENCE_FILL_QTY_UNPROVEN "
                            "contract=%s mode=%s — refusing historical donation",
                            self.client_id, contract_u, mode,
                        )
                        return None
                    if _coerce_utc(row.get("filled_ts")) is None:
                        log.critical(
                            "[%s] FILLED_ENTRY_EVIDENCE_FILL_TS_UNPROVEN "
                            "contract=%s mode=%s — refusing historical donation",
                            self.client_id, contract_u, mode,
                        )
                        return None
                    validated.append(row)
                return validated

            if position_id:
                def _linked_query():
                    with conn() as c:
                        c.execute(
                            columns
                            + """
                              AND position_id = %s
                            ORDER BY filled_ts DESC NULLS LAST,
                                     updated_ts DESC NULLS LAST,
                                     id DESC
                            """,
                            (self.client_id, mode, contract_u, position_id),
                        )
                        return [dict(r) for r in (c.fetchall() or [])]

                linked = _validated_rows(run_with_retry(_linked_query))
                if linked is None:
                    return None
                if len(linked) == 1:
                    return _decorate(linked[0])
                if len(linked) > 1:
                    log.critical(
                        "[%s] FILLED_ENTRY_EVIDENCE_LINKED_AMBIGUOUS "
                        "contract=%s pos=%s candidates=%d — refusing lifecycle cross-link",
                        self.client_id, contract_u, position_id, len(linked),
                    )
                    return None

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
                              AND (position_id IS NULL OR position_id = '')
                            ORDER BY filled_ts DESC NULLS LAST,
                                     updated_ts DESC NULLS LAST,
                                     id DESC
                            """,
                            (self.client_id, mode, contract_u),
                        )
                        return [dict(r) for r in (c.fetchall() or [])]

                unlinked = _validated_rows(run_with_retry(_unlinked_query))
                if unlinked is None:
                    return None
                candidates = []
                for row in unlinked:
                    filled_ts = _coerce_utc(row.get("filled_ts"))
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

            log.critical(
                "[%s] FILLED_ENTRY_EVIDENCE_UNSCOPED_BLOCKED contract=%s mode=%s "
                "— canonical position identity is required",
                self.client_id, contract_u, mode,
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
        entry_evidence: dict | None = None,
    ) -> float:
        """Return historical entry truth only; never substitute the current market."""
        for key in (
            "underlying_entry",
            "entry_underlying",
            "underlying_price_at_entry",
            "underlying_entry_price",
            "entry_underlying_price",
        ):
            val = _positive_float(pos.get(key))
            if val > 0:
                return val

        position_id = str(pos.get("id") or pos.get("position_id") or "").strip()
        evidence = entry_evidence
        if evidence is None:
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
            "underlying_price_at_entry", "entry_underlying_price",
        ):
            val = _positive_float(bp.get(key))
            if val > 0:
                return val

        # Without a canonical position timestamp/identity, an order matched only
        # by client + mode + OCC contract could belong to a prior economic lifecycle.
        return 0.0

    def _seed_exit_engine_from_position(self, pos: dict) -> None:
        """Seed/adopt an existing canonical DB position with its durable identity."""
        if not pos:
            return
        pos_id = str(pos.get("id") or pos.get("position_id") or "").strip()
        if pos_id.lower().startswith("broker-repair-"):
            log.critical(
                "[%s] RECONCILER_CANONICAL_SEED_BLOCKED synthetic_position_id=%s",
                self.client_id, pos_id,
            )
            return
        expected_client = str(self.client_id or "").strip()
        row_client = str(pos.get("client_id") or "").strip()
        mode = _strict_execution_mode(self.execution_mode)
        row_mode = _strict_execution_mode(pos.get("execution_mode"))
        if not expected_client or row_client != expected_client:
            log.critical(
                "[%s] RECONCILER_CANONICAL_SEED_BLOCKED client_identity_unproven pos=%s",
                self.client_id, pos_id or "?",
            )
            return
        if mode is None or row_mode is None or row_mode != mode:
            log.critical(
                "[%s] RECONCILER_CANONICAL_SEED_BLOCKED execution_mode_unproven "
                "pos=%s row_mode=%r reconciler_mode=%r",
                self.client_id, pos_id or "?", pos.get("execution_mode"), self.execution_mode,
            )
            return
        contract = self._norm_contract(pos.get("contract") or pos.get("symbol") or "")
        underlying = self._norm_underlying(
            pos.get("underlying") or pos.get("ticker") or self._norm_underlying(contract)
        )
        side = _strict_option_side(
            contract, pos.get("direction") or pos.get("side")
        )
        if side is None:
            log.critical(
                "[%s] RECONCILER_CANONICAL_SEED_BLOCKED direction_unproven "
                "pos=%s contract=%s persisted_direction=%r",
                self.client_id, pos_id or "?", contract,
                pos.get("direction") or pos.get("side"),
            )
            return
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
            pos,
            underlying=underlying,
            contract=contract,
            entry_evidence=evidence,
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
        client_id = str(self.client_id or "").strip()
        contract = self._norm_contract(contract)
        if not client_id or not contract:
            log.critical(
                "[%s] EXIT_ENGINE_CANONICAL_ADOPTION_BLOCKED client_or_contract_unproven "
                "contract=%s pos=%s",
                self.client_id, contract, pos_id or "?",
            )
            return
        ee = getattr(self, "exit_engine", None)
        if not ee:
            log.critical(
                "[%s] EXIT_ENGINE_NOT_WIRED contract=%s pos=%s "
                "— refusing reconciler seed",
                self.client_id, contract, pos_id or "?",
            )
            try:
                self._record_reconciler_rejection(
                    signal_id=str(pos_id or f"reconciled:{contract}"),
                    ticker=self._norm_underlying(underlying or contract),
                    category_name="HEALTH",
                    severity_name="CRITICAL",
                    reason_code="EXIT_ENGINE_NOT_WIRED",
                    human_reason="reconciler could not seed imported/open position because exit_engine is not wired",
                    contract=contract,
                    pos_id=pos_id,
                    qty=qty,
                    entry_px=entry_px,
                    price_untrusted=price_untrusted,
                    underlying_entry=underlying_entry,
                )
            except Exception:
                pass
            return

        try:
            from ap_exit_engine import ManagedPosition

            mode = _strict_execution_mode(self.execution_mode)
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
            if not callable(adopt):
                log.critical(
                    "[%s] EXIT_ENGINE_CANONICAL_ADOPTION_BLOCKED contract=%s pos=%s "
                    "reason=adoption_api_unavailable — refusing generic seed",
                    self.client_id, contract, pos_id,
                )
                return

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
                client_id=client_id,
                underlying_entry=float(persisted_underlying),
                score=0.0,
                tier="RECONCILED",
                pattern="",
                direction=side,
                timeframe="",
                underlying_stop=float(stop_u),
                underlying_target=float(target_u),
            )
            disposition = getattr(result, "disposition", None)
            adopted = getattr(result, "adopted", None)
            retryable = getattr(result, "retryable", None)
            safe_to_seed = getattr(result, "safe_to_seed", None)
            well_formed = (
                isinstance(disposition, str)
                and isinstance(adopted, bool)
                and isinstance(retryable, bool)
                and isinstance(safe_to_seed, bool)
            )

            if (
                well_formed
                and disposition in {"ADOPTED", "ALREADY_CANONICAL_REPAIR_REMOVED"}
                and adopted is True
                and retryable is False
                and safe_to_seed is False
            ):
                log.critical(
                    "[%s] EXIT_ENGINE_CANONICAL_ADOPTED_FROM_RECONCILER contract=%s pos=%s disposition=%s underlying_entry=%s source=%s",
                    self.client_id, contract, pos_id, disposition,
                    persisted_underlying if persisted_underlying > 0 else "unknown",
                    evidence.get("underlying_entry_source") or "db_position",
                )
                return

            if (
                well_formed
                and disposition == "NO_REPAIR_FOUND"
                and adopted is False
                and retryable is False
                and safe_to_seed is True
            ):
                pass
            else:
                log.critical(
                    "[%s] EXIT_ENGINE_CANONICAL_ADOPTION_HOLD contract=%s pos=%s "
                    "disposition=%r adopted=%r retryable=%r safe_to_seed=%r "
                    "— refusing second owner",
                    self.client_id,
                    contract,
                    pos_id,
                    disposition,
                    adopted,
                    retryable,
                    safe_to_seed,
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
            mp.client_id = client_id
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

globals()["_strict_option_side"] = _strict_option_side
globals()["_OCC_CP_RE"] = _OCC_CP_RE
