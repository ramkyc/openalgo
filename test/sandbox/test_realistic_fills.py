"""Tests for sandbox/realistic_fills.py and its engine wiring (flag-guarded)."""

from datetime import datetime
from decimal import Decimal as D
from types import SimpleNamespace
from unittest import mock

import pytz

from sandbox import realistic_fills as rf
from sandbox.execution_engine import ExecutionEngine

IST = pytz.timezone("Asia/Kolkata")


def at(h, m):
    return IST.localize(datetime(2026, 10, 14, h, m))


def test_slippage_tiers():
    assert rf.slippage_rate(at(9, 20)) == D("0.003")
    assert rf.slippage_rate(at(11, 0)) == D("0.001")
    assert rf.slippage_rate(at(15, 10)) == D("0.002")


def test_adverse_fill_uses_touch_when_depth_real():
    assert rf.adverse_fill("BUY", D("100"), D("99.9"), D("100.1"), at(11, 0)) == D("100.1")
    assert rf.adverse_fill("SELL", D("100"), D("99.9"), D("100.1"), at(11, 0)) == D("99.9")


def test_adverse_fill_slippage_when_bid_ask_equal_ltp():
    # WebSocket mock quote: bid == ask == ltp
    assert rf.adverse_fill("BUY", D("100"), D("100"), D("100"), at(11, 0)) == D("100.10")
    assert rf.adverse_fill("SELL", D("100"), D("100"), D("100"), at(11, 0)) == D("99.90")


def test_stop_fill_overshoot_and_touch():
    assert rf.stop_fill("BUY", D("167.88"), None, at(11, 35)) == D("168.05")
    assert rf.stop_fill("BUY", D("167.88"), D("171.8"), at(11, 35)) == D("171.8")
    # touch already retraced below trigger -> still pays trigger + overshoot
    assert rf.stop_fill("BUY", D("167.88"), D("160"), at(11, 35)) == D("168.05")
    assert rf.stop_fill("SELL", D("100"), D("98"), at(11, 0)) == D("98")


def test_watchdog_14_oct_pe_spike_between_polls():
    wd = rf.StopWatchdog()
    trig = D("167.88")
    # first sighting: day high below trigger, baseline recorded
    assert not wd.crossed("o1", "BUY", trig, {"high": 160.0, "ltp": 150.0})
    # spike to 171.8 happened between polls; LTP has retraced to 150
    assert wd.crossed("o1", "BUY", trig, {"high": 171.8, "ltp": 150.0})


def test_watchdog_ignores_pre_existing_extreme():
    wd = rf.StopWatchdog()
    assert not wd.crossed("o2", "BUY", D("167.88"), {"high": 172.6})
    assert not wd.crossed("o2", "BUY", D("167.88"), {"high": 173.0})


def test_watchdog_sell_stop_uses_low():
    wd = rf.StopWatchdog()
    assert not wd.crossed("o3", "SELL", D("90"), {"low": 95})
    assert wd.crossed("o3", "SELL", D("90"), {"low": 89.5})


def _order(**kw):
    base = dict(
        orderid="X1", symbol="NIFTY14OCT2624700PE", exchange="NFO", action="BUY",
        price_type="SL-M", trigger_price=D("167.88"), price=D("0"),
        order_status="trigger pending", quantity=75, product="MIS",
    )
    base.update(kw)
    return SimpleNamespace(**base)


def _engine():
    eng = ExecutionEngine()
    eng._stop_watchdog = rf.StopWatchdog()
    eng._execute_order = mock.Mock()
    eng._fetch_quote = mock.Mock(return_value=None)
    return eng


def _run(eng, order, quote, enabled):
    with mock.patch("sandbox.execution_engine.SandboxTrades") as t, mock.patch.object(
        rf, "is_enabled", return_value=enabled
    ), mock.patch("sandbox.execution_engine.quote_looks_stale", return_value=False):
        t.query.filter_by.return_value.first.return_value = None
        eng._process_order(order, quote)


def test_engine_flag_off_misses_spike():
    eng, order = _engine(), _order()
    _run(eng, order, {"ltp": 150.0, "high": 160.0, "bid": 149.9, "ask": 150.1}, False)
    _run(eng, order, {"ltp": 150.0, "high": 171.8, "bid": 149.9, "ask": 150.1}, False)
    eng._execute_order.assert_not_called()


def test_engine_flag_on_fires_stop_with_overshoot():
    eng, order = _engine(), _order()
    _run(eng, order, {"ltp": 150.0, "high": 160.0, "bid": 149.9, "ask": 150.1}, True)
    eng._execute_order.assert_not_called()
    _run(eng, order, {"ltp": 150.0, "high": 171.8, "bid": 149.9, "ask": 150.1}, True)
    eng._execute_order.assert_called_once()
    assert eng._execute_order.call_args[0][1] >= D("167.88")
