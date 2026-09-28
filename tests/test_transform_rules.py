"""
test_transform_rules.py
-------------------------------------------------------------------------
Unit tests for the pure business-rule logic in
notebooks/03_transaction_incremental_transform.py, re-implemented here in
plain Python (no PySpark/Spark session required) so they run in a few
milliseconds as part of a normal `pytest` CI step — Spark integration
testing for the full notebook is out of scope for this file by design.

Run with:
  pytest tests/test_transform_rules.py -v           # Databricks UI test runner
  PYTHONDONTWRITEBYTECODE=1 python -m pytest tests/   # CLI (required on DABs FUSE mount)
  ./scripts/run_tests.sh                              # wrapper script (sets env for you)
"""

from dataclasses import dataclass
from typing import Optional


# --- Reimplementation of the notebook's business rules, pure Python ---------

def transaction_sign(drcr_ind: str) -> str:
    return {"C": "+", "D": "-"}.get(drcr_ind, "?")


def is_negative_amount(source_lcy_amount: float) -> bool:
    return source_lcy_amount < 0


def abs_lcy_amount(source_lcy_amount: float) -> float:
    return abs(source_lcy_amount)


def net_signed_amount(drcr_ind: str, source_lcy_amount: float) -> float:
    magnitude = abs(source_lcy_amount)
    if drcr_ind == "C":
        return magnitude
    if drcr_ind == "D":
        return -magnitude
    return 0.0


def is_reversal_candidate(source_lcy_amount: float, is_reversal_code: bool, map_module: str) -> bool:
    """A negative amount ALONE is never a reversal — it also needs a verified
    reversal code/module mapping. This is the rule the 04b quality gate protects."""
    return is_negative_amount(source_lcy_amount) and (is_reversal_code or map_module == "REVERSAL")


@dataclass
class MappingRule:
    trn_code: Optional[str]
    module: Optional[str]
    product: Optional[str]
    priority: int
    map_module: str
    is_reversal_code: bool = False


def classify(trn_code: str, module: str, product: str, rules: list[MappingRule]):
    """Mirrors notebook 03's match-score + priority-tiebreak classification precedence:
    TRN_CODE+MODULE+PRODUCT (400) > TRN_CODE+MODULE (300) > TRN_CODE (200) >
    MODULE+PRODUCT (150) > MODULE (100) > unmapped (0)."""
    best = None
    best_score = -1
    for rule in rules:
        matches = (
            (rule.trn_code is None or rule.trn_code == trn_code)
            and (rule.module is None or rule.module == module)
            and (rule.product is None or rule.product == product)
        )
        if not matches:
            continue
        if rule.trn_code and rule.module and rule.product:
            score = 400
        elif rule.trn_code and rule.module:
            score = 300
        elif rule.trn_code:
            score = 200
        elif rule.module and rule.product:
            score = 150
        elif rule.module:
            score = 100
        else:
            score = 0
        if score > best_score or (score == best_score and best and rule.priority > best.priority):
            best, best_score = rule, score
    if best is None:
        return "UNMAPPED", "UNMAPPED"
    status = {
        400: "MAPPED_TRN_MODULE_PRODUCT", 300: "MAPPED_TRN_MODULE", 200: "MAPPED_TRN_CODE",
        150: "MAPPED_MODULE_PRODUCT", 100: "MAPPED_MODULE",
    }[best_score]
    return best.map_module, status


# --- Tests -------------------------------------------------------------------

def test_transaction_sign_credit_is_positive():
    assert transaction_sign("C") == "+"


def test_transaction_sign_debit_is_negative():
    assert transaction_sign("D") == "-"


def test_transaction_sign_unknown_indicator_is_question_mark():
    assert transaction_sign("X") == "?"


def test_abs_lcy_amount_is_always_positive():
    assert abs_lcy_amount(-1500.50) == 1500.50
    assert abs_lcy_amount(1500.50) == 1500.50


def test_net_signed_amount_credit_positive_debit_negative():
    assert net_signed_amount("C", 1000) == 1000
    assert net_signed_amount("D", 1000) == -1000
    # sign of the SOURCE amount must not matter — only DRCR_IND decides the sign.
    assert net_signed_amount("D", -1000) == -1000


def test_negative_amount_alone_is_not_a_reversal_candidate():
    assert is_reversal_candidate(-500, is_reversal_code=False, map_module="DEPOSIT") is False


def test_reversal_requires_negative_amount_and_verified_code():
    assert is_reversal_candidate(-500, is_reversal_code=True, map_module="REVERSAL") is True
    # positive amount on a verified reversal code is still not a reversal candidate
    assert is_reversal_candidate(500, is_reversal_code=True, map_module="REVERSAL") is False


def test_classification_precedence_trn_module_product_beats_trn_module():
    rules = [
        MappingRule(trn_code="F23", module=None, product=None, priority=100, map_module="FUNDS_TRANSFER"),
        MappingRule(trn_code="F23", module="FT", product="FT", priority=50, map_module="SPECIFIC_FT_PRODUCT"),
    ]
    map_module, status = classify("F23", "FT", "FT", rules)
    assert map_module == "SPECIFIC_FT_PRODUCT"
    assert status == "MAPPED_TRN_MODULE_PRODUCT"


def test_classification_falls_back_to_module_only_mapping():
    rules = [MappingRule(trn_code=None, module="DE", product=None, priority=10, map_module="DEPOSIT")]
    map_module, status = classify("XYZ", "DE", "SOMETHING", rules)
    assert map_module == "DEPOSIT"
    assert status == "MAPPED_MODULE"


def test_unmapped_combination_falls_through_cleanly():
    rules = [MappingRule(trn_code=None, module="DE", product=None, priority=10, map_module="DEPOSIT")]
    map_module, status = classify("ZZZ", "UNKNOWN_MODULE", "UNKNOWN_PRODUCT", rules)
    assert map_module == "UNMAPPED"
    assert status == "UNMAPPED"
