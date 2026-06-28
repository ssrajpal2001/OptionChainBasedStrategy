"""
strategies/core/rule_evaluator.py — reusable rule-builder evaluator.

Extracted from strategies/sell_straddle.py. Evaluates admin-configured rule
chains against per-timeframe indicator dictionaries.
"""
from __future__ import annotations

import logging
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)


def _compare(v1: float, v2: float, sym: str) -> bool:
    if sym == ">":
        return v1 > v2
    if sym == "<":
        return v1 < v2
    if sym == ">=":
        return v1 >= v2
    if sym == "<=":
        return v1 <= v2
    if sym == "==":
        return abs(v1 - v2) < 1e-9
    return False


def eval_rules(rules: List[dict], ind_by_tf: Dict[int, Dict[str, float]]) -> Tuple[bool, str]:
    """
    Evaluate admin rule-builder rules against per-timeframe indicator values.

    ``ind_by_tf`` maps {tf:int -> {operand:value}}. Each rule is evaluated
    against the indicators resampled to THAT rule's ``tf`` (falling back to
    tf=1).

    Backward compat: if a flat single-tf dict {operand:value} is passed, it is
    wrapped as {1: ind} so old callers keep working.
    """
    if not rules:
        return True, "No rules — always allowed"

    # Backward-compat: flat {operand:value} dict -> treat as tf=1
    if ind_by_tf and not isinstance(next(iter(ind_by_tf.values())), dict):
        ind_by_tf = {1: ind_by_tf}

    tokens: List[str] = []
    reasons: List[str] = []

    for i, rule in enumerate(rules):
        try:
            _tf = int(rule.get("tf", 1))
        except Exception:
            _tf = 1
        ind = ind_by_tf.get(_tf) or ind_by_tf.get(1, {})

        indicator = (rule.get("indicator") or "").lower()
        op_sym = rule.get("operator_sym", "<")
        passed = False
        label = ""

        if indicator == "advanced":
            op1 = (rule.get("operand1") or "").lower()
            op2 = (rule.get("operand2") or "").lower()
            v1 = ind.get(op1)
            v2 = float(rule.get("operand2_val", 0)) if op2 == "value" else ind.get(op2)
            if v1 is not None and v2 is not None:
                passed = _compare(v1, v2, op_sym)
            v1s = f"{v1:.2f}" if isinstance(v1, float) else "N/A"
            v2s = f"{v2:.2f}" if isinstance(v2, float) else "N/A"
            label = f"{op1.upper()}({v1s}){op_sym}{op2.upper()}({v2s})"
        else:
            val = ind.get(indicator)
            thr = float(rule.get("threshold", 0))
            if val is not None:
                passed = _compare(val, thr, op_sym)
            lv = f"{val:.2f}" if isinstance(val, float) else "N/A"
            label = f"{indicator.upper()}({lv}){op_sym}{thr}"

        reasons.append(f"{label}={'✓' if passed else '✗'}")

        for b in str(rule.get("openBrackets", "")):
            tokens.append(b)
        tokens.append("True" if passed else "False")
        for b in str(rule.get("closeBrackets", "")):
            tokens.append(b)
        if i < len(rules) - 1:
            op = (rule.get("operator") or "AND").upper()
            tokens.append("and" if op == "AND" else "or")

    try:
        result = bool(eval(" ".join(tokens)))  # noqa: S307
    except Exception as exc:
        logger.error("Rule eval error: %s tokens=%s", exc, tokens)
        result = False

    return result, " | ".join(reasons)


class RuleEvaluator:
    """Thin wrapper around :func:`eval_rules` for callers that prefer OOP."""

    def __init__(self, rules: List[dict] | None = None) -> None:
        self.rules = rules or []

    def evaluate(self, ind_by_tf: Dict[int, Dict[str, float]]) -> Tuple[bool, str]:
        return eval_rules(self.rules, ind_by_tf)
