"""
Telegram alert for the monthly momentum screeners (NIFTY50, NIFTY Microcap).

Sent once, the moment a screener persists a month's finalized target list
(the evening of the month's last trading day), so the rebalance list is in
hand before the next session's open -- the validated execution point.
Informational only: lists what to BUY (in the new list, not yet held) and
SELL (held, dropped from the list). Actual trades are still placed manually
and confirmed on the dashboard; nothing here touches positions.

Text is kept free of Markdown control characters (* _ ` [) because
telegram_notifier.send_sync() always sends with parse_mode=Markdown.
"""

import logging

from live_trading.shared.telegram_notifier import send_sync

logger = logging.getLogger(__name__)


def notify_finalized_list(screener_name: str, rebalance_month: str, signal_date: str,
                          rows: list[dict], open_positions: list[dict], top_label: str) -> bool:
    """Send the month's BUY/SELL diff. Never raises -- a failed alert must
    not break the scan cycle that already persisted the list."""
    try:
        target = {r["symbol"]: r for r in rows}
        held = {p["symbol"]: p for p in open_positions}
        buys = [target[s] for s in target if s not in held]
        sells = [held[s] for s in held if s not in target]
        keep = sorted(s for s in target if s in held)

        lines = [
            f"{screener_name}: {rebalance_month} rebalance list FINALIZED",
            f"Signal: {signal_date} close ({len(target)} names, {top_label}). "
            f"Execute at the next session's open.",
            "",
            f"BUY ({len(buys)}):",
        ]
        lines += [f"  {r['symbol']}  {r['shares']} sh @ ~{r['ref_price']}" for r in buys] or ["  none"]
        lines += ["", f"SELL ({len(sells)}):"]
        lines += [f"  {p['symbol']}  {p['qty']} sh" for p in sells] or ["  none"]
        lines += ["", f"KEEP ({len(keep)}): {', '.join(keep) if keep else 'none'}",
                  "", "Confirm actual fills on the dashboard."]
        ok = send_sync("\n".join(lines))
        if not ok:
            logger.warning(f"{screener_name}: Telegram rebalance alert not sent "
                           f"(missing token/chat id or send failed)")
        return ok
    except Exception as e:
        logger.error(f"{screener_name}: Telegram rebalance alert failed: {e}")
        return False
