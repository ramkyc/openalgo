"""
Regression guard for Fyers HSM paise→rupees conversion.

Fyers HSM streams EVERY instrument — equities, F&O, AND indices
(NIFTY/BANKNIFTY/SENSEX/INDIAVIX) — with prices in paise (100× the real
value). A latent bug special-cased indices (`is_index` / an exchange
whitelist that omitted NSE_INDEX/BSE_INDEX), so index LTPs leaked through
100× inflated whenever a new feed path was wired up (e.g. NIFTY showing
24,15,105 instead of 24,151.05; INDIAVIX 1306 instead of 13.06).

These tests pin the conversion for every mapper path so the bug can never
silently come back. If one fails, do NOT add a divisor branch — fix
hsm_price_to_rupees() instead (the single source of truth).

Run: uv run pytest test/test_fyers_price_conversion.py -v
"""

import pytest

from broker.fyers.streaming.fyers_mapping import (
    FyersDataMapper,
    hsm_price_to_rupees,
)

# (label, raw_paise_value, expected_rupees) — multiplier is 1 for all HSM
# instruments we trade; precision is 2.
CASES = [
    ("NIFTY index", 2_415_105, 24_151.05),
    ("BANKNIFTY index", 5_777_975, 57_779.75),
    ("SENSEX index", 7_725_487, 77_254.87),
    ("INDIAVIX index", 1_306, 13.06),
    ("NIFTY option", 54_585, 545.85),
    ("equity", 123_45, 123.45),
]


@pytest.mark.parametrize("label, raw, expected", CASES)
def test_hsm_price_to_rupees(label, raw, expected):
    assert hsm_price_to_rupees(raw, multiplier=1, precision=2) == expected


def test_zero_and_falsy_inputs():
    assert hsm_price_to_rupees(0) == 0.0
    assert hsm_price_to_rupees(None) == 0.0


def test_missing_multiplier_defaults_to_one():
    # A truncated index packet may omit `multiplier`; default must be 1 so the
    # value is divided by 100 only (not 100×100).
    assert hsm_price_to_rupees(2_415_105) == 24_151.05


def test_index_quote_path_not_inflated():
    """Index Quote fan-out (FyersAdapter -> map_fyers_data 'Quote')."""
    mapper = FyersDataMapper()
    out = mapper.map_to_openalgo_quote(
        {
            "type": "if",
            "original_symbol": "NSE_INDEX:NIFTY",
            "ltp": 2_415_105,
            "multiplier": 1,
            "precision": 2,
        }
    )
    assert out["ltp"] == 24_151.05


def test_index_synthetic_depth_path_not_inflated():
    """Index Depth fan-out (FyersAdapter -> map_index_to_synthetic_depth).

    This was the worst offender — it divided by `multiplier` only, dropping
    the /100 entirely.
    """
    mapper = FyersDataMapper()
    out = mapper.map_index_to_synthetic_depth(
        {
            "type": "if",
            "original_symbol": "NSE_INDEX:NIFTY",
            "ltp": 2_415_105,
            "multiplier": 1,
            "precision": 2,
        }
    )
    assert out["ltp"] == 24_151.05


def test_index_ltp_path_not_inflated():
    mapper = FyersDataMapper()
    out = mapper.map_to_openalgo_ltp(
        {
            "type": "if",
            "original_symbol": "NSE_INDEX:NIFTY",
            "ltp": 2_415_105,
            "multiplier": 1,
            "precision": 2,
        }
    )
    assert out["ltp"] == 24_151.05


def test_sensex_bse_index_not_inflated():
    mapper = FyersDataMapper()
    out = mapper.map_to_openalgo_quote(
        {
            "type": "if",
            "original_symbol": "BSE_INDEX:SENSEX",
            "ltp": 7_725_487,
            "multiplier": 1,
            "precision": 2,
        }
    )
    assert out["ltp"] == 77_254.87


def test_option_quote_unchanged():
    """Options (which already worked) must stay correct after the refactor."""
    mapper = FyersDataMapper()
    out = mapper.map_to_openalgo_quote(
        {
            "type": "sf",
            "original_symbol": "NFO:NIFTY25JUN24000CE",
            "ltp": 54_585,
            "multiplier": 1,
            "precision": 2,
        }
    )
    assert out["ltp"] == 545.85
