"""Study-identical GEX math (pure python/numpy, no numba).

Mirrors research/gex_ict_confluence_study/gex_profile.py::compute_gex_profile:
  r = 0.07, F = spot * exp(r*T), T = (expiry 15:30 - now) / 365d,
  IV via Black-76 (Newton w/ bisection fallback), gamma per (strike, type),
  gex = oi * gamma * lot * spot^2 * 0.01, CE +, PE -.
Returns per-strike rows shaped for the bot's _derive_gex_levels
(strike, ce_gex, pe_gex, net_gex). Input is the OpenAlgo option-chain rows
(strike, ce{ltp,oi,lotsize}, pe{...}).
"""
from __future__ import annotations

import math
from datetime import datetime

RATE = 0.07
_SQ2PI = math.sqrt(2 * math.pi)


def _ncdf(x: float) -> float:
    return 0.5 * math.erfc(-x / math.sqrt(2))


def _price(F, K, T, s, opt):
    d1 = (math.log(F / K) + 0.5 * s * s * T) / (s * math.sqrt(T))
    d2 = d1 - s * math.sqrt(T)
    disc = math.exp(-RATE * T)
    if opt == "CE":
        return disc * (F * _ncdf(d1) - K * _ncdf(d2))
    return disc * (K * _ncdf(-d2) - F * _ncdf(-d1))


def _vega(F, K, T, s):
    d1 = (math.log(F / K) + 0.5 * s * s * T) / (s * math.sqrt(T))
    return math.exp(-RATE * T) * F * math.exp(-0.5 * d1 * d1) / _SQ2PI * math.sqrt(T)


def _gamma(F, K, T, s):
    d1 = (math.log(F / K) + 0.5 * s * s * T) / (s * math.sqrt(T))
    return math.exp(-RATE * T) * math.exp(-0.5 * d1 * d1) / _SQ2PI / (F * s * math.sqrt(T))


def _iv(price, F, K, T, opt, tol=1e-7, max_iter=100):
    """Same algorithm as backtesting/iron_fly/greeks_engine.black76_iv."""
    if T <= 0 or price <= 1e-4:
        return 0.0
    a, b = 1e-4, 5.0
    if price <= _price(F, K, T, a, opt):
        return a
    if price >= _price(F, K, T, b, opt):
        return b
    s = math.sqrt(2 * math.pi / T) * (price / F)
    if s < a or s > b:
        s = 0.2
    for _ in range(max_iter):
        diff = price - _price(F, K, T, s, opt)
        if abs(diff) < tol:
            return s
        v = _vega(F, K, T, s)
        if abs(v) > 1e-8:
            step = max(-0.5, min(0.5, diff / v))
            s += step
        else:
            if diff > 0:
                a = s
            else:
                b = s
            s = (a + b) / 2.0
        if s <= 0:
            s = 1e-4
        if s > 5.0:
            s = 5.0
    return s


def study_gex_chain(spot: float, chain: list[dict], expiry_dt: datetime,
                    now: datetime) -> list[dict]:
    """expiry_dt/now are naive IST datetimes (expiry at 15:30)."""
    T = (expiry_dt - now).total_seconds() / (365 * 24 * 3600)
    if T <= 0 or spot <= 0:
        return []
    F = spot * math.exp(RATE * T)
    k2 = spot * spot * 0.01
    out = []
    for item in chain:
        K = float(item["strike"])
        row = {"strike": K, "ce_gex": 0.0, "pe_gex": 0.0}
        for side, key in (("CE", "ce"), ("PE", "pe")):
            leg = item.get(key) or {}
            ltp = leg.get("ltp") or 0
            oi = leg.get("oi") or 0
            lot = leg.get("lotsize") or 1
            if ltp <= 0 or oi <= 0:
                continue
            iv = _iv(float(ltp), F, K, T, side)
            if iv <= 0:
                continue
            row[f"{key}_gex"] = oi * _gamma(F, K, T, iv) * lot * k2
        row["net_gex"] = row["ce_gex"] - row["pe_gex"]
        out.append(row)
    return out
