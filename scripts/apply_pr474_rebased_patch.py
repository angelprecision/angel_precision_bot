from pathlib import Path
from textwrap import indent

CORE = Path("ap_execution_core.py")
HELPERS = Path("scripts/pr474_helper_block.txt")
FINAL_BLOCK = Path("scripts/pr474_final_block.txt")
SEAM4 = Path("tests/test_p0_seam4_e2e_deferred_lifecycle.py")
P0_WORKFLOW = Path(".github/workflows/p0_regression.yml")


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected exactly 1 anchor, found {count}")
    return text.replace(old, new, 1)


text = CORE.read_text()

text = replace_once(
    text,
    "import json\nimport math\n",
    "import copy\nimport json\nimport math\n",
    "import copy",
)

helper_block = HELPERS.read_text().rstrip()
text = replace_once(
    text,
    "    return bool(is_occ and price > 0 and qty > 0), contract, price, qty\n\nDISCORD_WEBHOOK_URL =",
    "    return bool(is_occ and price > 0 and qty > 0), contract, price, qty\n\n\n"
    + helper_block
    + "\n\nDISCORD_WEBHOOK_URL =",
    "helper insertion",
)

text = replace_once(
    text,
    '''        if approved_plan is not None and self.master_control is not None:
            try:
                reval = self.master_control.revalidate_exposure(
                    approved_plan,
                    client_id=self.email or "default",
                )
''',
    '''        _deferred_placeholder = False
        if approved_plan is not None:
            _deferred_placeholder, _blank_contract_malformed = (
                _classify_deferred_plan(approved_plan, sig)
            )
            if _deferred_placeholder:
                log.info(
                    "[%s] Breach exposure cost revalidation deferred until "
                    "real OCC materialization; kill-switch and slot checks passed",
                    ticker,
                )

        if (
            approved_plan is not None
            and self.master_control is not None
            and not _deferred_placeholder
        ):
            try:
                reval = self.master_control.revalidate_exposure(
                    approved_plan,
                    client_id=self.email or "default",
                )
''',
    "breach deferred revalidation gate",
)

text = replace_once(
    text,
    '''        _deferred   = (
            bool(_sig_meta.get("contract_deferred"))
            or bool(_sig_dict.get("contract_deferred"))
            or not _contract_sym_raw
            or _contract_sym_raw.upper().startswith("DEFERRED:")  # safety: never submit placeholder
        )
        # Enable deferred-outcome emission only for deferred triggers (amendment:
''',
    '''        _deferred, _blank_contract_malformed = _classify_deferred_plan(
            approved_plan,
            sig,
        )
        if _blank_contract_malformed:
            _blank_reason = "MALFORMED_BLANK_CONTRACT_WITHOUT_DEFERRED_PROVENANCE"
            log.critical(
                "[%s] PRODUCTION_ENTRY_BLOCK — %s; refusing to treat a missing "
                "contract as deferred authority",
                ticker,
                _blank_reason,
            )
            return _terminalize_breach_failure(
                _blank_reason,
                cleanup_action="expire",
                meta_patch={
                    "failure_stage": "deferred_classification",
                    "contract_symbol": _contract_sym_raw,
                    "contract_deferred": False,
                },
                context_notes=_blank_reason,
            )
        # Enable deferred-outcome emission only for deferred triggers (amendment:
''',
    "strict deferred classification",
)

text = replace_once(
    text,
    '''                            _prem_per_contract = float(getattr(_sel, "premium_per_contract", 0) or 0)
                            if _prem_per_contract > 0:
                                approved_plan.max_position_usd = _sel_qty * _prem_per_contract
''',
    '''                            approved_plan.max_position_usd = round(
                                float(_sel_price_candidate)
                                * int(_sel_qty)
                                * 100.0,
                                2,
                            )
''',
    "selector deterministic cost",
)

text = replace_once(
    text,
    '''                        _prem = float(getattr(_sel, "premium_per_contract", 0) or 0)
                        if _prem > 0:
                            approved_plan.max_position_usd = _prem
                        log.info(
                            "[%s] DEFERRED_ACCEPTANCE_CAP_PASS contract=%s "
                            "ask=%.2f cap=%.2f qty=1",
                            ticker, _sel_contract, _sel_ask, _accept_cap,
                        )
''',
    '''                        approved_plan.max_position_usd = round(
                            float(_sel_price_candidate) * 1 * 100.0,
                            2,
                        )
                        log.info(
                            "[%s] DEFERRED_ACCEPTANCE_CAP_PASS contract=%s "
                            "ask=%.2f cap=%.2f qty=1 cost=$%.2f",
                            ticker,
                            _sel_contract,
                            _sel_ask,
                            _accept_cap,
                            float(approved_plan.max_position_usd),
                        )
''',
    "acceptance deterministic cost",
)

final_block = indent(FINAL_BLOCK.read_text().rstrip(), "        ")
text = replace_once(
    text,
    '''        # Keep approved_plan in sync so OSM and DB record the correct price.
        try:
            approved_plan.limit_price = submit_limit
        except Exception:
            pass  # plan is a namespace; attribute assignment is always valid

        # Build the entry pricing audit to persist in orders.meta post-submit.
''',
    '''        # Keep approved_plan in sync so OSM and DB record the correct price.
        try:
            approved_plan.limit_price = submit_limit
        except Exception:
            pass  # plan is a namespace; attribute assignment is always valid

'''
    + final_block
    + '''

        # Build the entry pricing audit to persist in orders.meta post-submit.
''',
    "final broker-ready exposure revalidation",
)

CORE.write_text(text)

seam4 = SEAM4.read_text()
seam4 = replace_once(
    seam4,
    '''        master_control=types.SimpleNamespace(mode="LIVE", max_positions=5, _kill_switch_fn=lambda: False),
''',
    '''        master_control=types.SimpleNamespace(
            mode="LIVE",
            max_positions=5,
            _kill_switch_fn=lambda: False,
            # PR #474: this integration harness now crosses the mandatory
            # final deferred exposure authority. Model the production method
            # explicitly instead of relying on an incomplete namespace.
            revalidate_exposure=lambda plan, client_id="default": types.SimpleNamespace(
                ok=True,
                reason="",
            ),
        ),
''',
    "seam4 final Master Control harness",
)
SEAM4.write_text(seam4)

p0 = P0_WORKFLOW.read_text()
p0_anchor = "            tests/test_p0_seam4_e2e_deferred_lifecycle.py \\\n"
p0 = replace_once(
    p0,
    p0_anchor,
    p0_anchor + "            tests/test_p0_deferred_real_cost_revalidation.py \\\n",
    "P0 CI focused registration",
)
P0_WORKFLOW.write_text(p0)

print("PR474 exact-anchor production patch applied")
