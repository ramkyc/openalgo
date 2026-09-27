"""A triggered SL-M fills at the far touch, not at the print that triggered it.

An SL-M order becomes a market order once its trigger is met, and a market
order pays the spread plus whatever the price does before it reaches the book.
The sandbox used to fill it at the triggering LTP, which on NIFTY ATM options
flattered every stop exit: the ask about a second after a stop print sat a
median 0.4% (mean 0.8%, p90 2.4%) above the trigger, measured on 601 replayed
stops (options_data atm_short_straddle_scalp_study, D19). The WebSocket engine
only carries LTP, so the fill price comes from a fresh quote taken on trigger.
"""

import inspect
from decimal import Decimal
from types import SimpleNamespace

from sandbox.execution_engine import ExecutionEngine


def _engine(quote):
    engine = ExecutionEngine.__new__(ExecutionEngine)
    engine._fetch_quote = lambda symbol, exchange: quote
    return engine


def _order(action):
    return SimpleNamespace(orderid="T1", symbol="X", exchange="NFO", action=action)


def test_buy_stop_pays_the_ask():
    engine = _engine({"ltp": 145.6, "bid": 145.65, "ask": 147.85})
    assert engine._stop_market_fill_price(_order("BUY"), Decimal("145.6")) == Decimal("147.85")


def test_sell_stop_receives_the_bid():
    engine = _engine({"ltp": 100.0, "bid": 99.2, "ask": 100.3})
    assert engine._stop_market_fill_price(_order("SELL"), Decimal("100.0")) == Decimal("99.2")


def test_no_quote_falls_back_to_the_trigger_print():
    """A failed quote must still fill the stop; leaving it open is worse."""
    engine = _engine(None)
    assert engine._stop_market_fill_price(_order("BUY"), Decimal("58.8")) == Decimal("58.8")


def test_a_quote_without_a_touch_falls_back():
    for q in ({"ltp": 58.8}, {"ltp": 58.8, "ask": 0}, {"ltp": 58.8, "ask": None}, {"ltp": 58.8, "ask": "x"}):
        assert _engine(q)._stop_market_fill_price(_order("BUY"), Decimal("58.8")) == Decimal("58.8")


def test_a_stale_quote_falls_back():
    engine = _engine({"ltp": 10.0, "high": 80.0, "low": 50.0, "ask": 10.5})
    assert engine._stop_market_fill_price(_order("BUY"), Decimal("58.8")) == Decimal("58.8")


def test_every_sl_m_fill_path_uses_it():
    """Placement, the trigger-pending check and the open-order check all fill SL-M."""
    from sandbox import order_manager

    for fn in (
        ExecutionEngine._process_order,
        ExecutionEngine._process_trigger_pending_order,
        order_manager.OrderManager.place_order,
    ):
        assert "_stop_market_fill_price" in inspect.getsource(fn), fn.__name__
