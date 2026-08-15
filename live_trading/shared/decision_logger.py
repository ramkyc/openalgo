"""
live_trading/shared/decision_logger.py
========================================
Shared per-bar decision-state JSONL logger + throttled heartbeat, so any
bot's entry / no-entry reasoning can be reconstructed after the fact — not
just the trades it actually took. Modeled on the pattern already proven in
banknifty_bb_options_bot.py (decisions.jsonl + "💓 DECISION STATE" log lines).

Usage (in any bot):
    from live_trading.shared.decision_logger import DecisionLogger

    self._dlog = DecisionLogger(LOGS_DIR / "<bot>_decisions.jsonl", heartbeat_secs=300)

    # on every bar close (cadence is up to the bot — once per signal-eligible bar):
    self._dlog.log_bar({
        "phase":     "ACTIVE" if self.active_trade else "WAITING",
        "in_window": in_window,
        ... any bot-specific fields (indicators, verdicts, active_trade) ...
    })

    # cheap to call on every tick — internally throttled to heartbeat_secs:
    self._dlog.maybe_heartbeat(lambda: self._build_heartbeat_text())

The DecisionLogger:
  • Appends one JSON line per log_bar() call — "ts" is added automatically
  • Swallows all exceptions — logging can never crash the bot
  • Heartbeat text is built lazily (via callback) only once the interval has
    actually elapsed, so bots don't pay the cost of formatting a status
    string on every tick
  • Heartbeat lines go through the bot's own logger (or module logger if none
    given) — no separate heartbeat file, just regular log lines
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Callable

logger = logging.getLogger(__name__)


class DecisionLogger:
    def __init__(
        self,
        jsonl_path: Path,
        heartbeat_secs: int = 300,
        bot_logger: logging.Logger | None = None,
    ):
        self.jsonl_path = Path(jsonl_path)
        self.heartbeat_secs = heartbeat_secs
        self._logger = bot_logger or logger
        self._last_heartbeat: datetime | None = None

    def log_bar(self, record: dict) -> None:
        """Append one decision snapshot line. Never raises."""
        try:
            full = {"ts": datetime.now().isoformat(timespec="seconds"), **record}
            with open(self.jsonl_path, "a") as f:
                f.write(json.dumps(full, default=str) + "\n")
        except Exception:
            pass  # never let logging kill the bot

    def maybe_heartbeat(self, build_text: Callable[[], str]) -> None:
        """Log build_text()'s lines, but only once per heartbeat_secs."""
        now = datetime.now()
        if (self._last_heartbeat is not None
                and (now - self._last_heartbeat).total_seconds() < self.heartbeat_secs):
            return
        self._last_heartbeat = now
        try:
            text = build_text()
        except Exception:
            return  # never let a broken heartbeat builder crash the bot
        for line in text.splitlines():
            self._logger.info(line)
