import importlib
import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple, Union

from database.auth_db import get_auth_token_broker
from database.token_db import get_token
from utils.constants import VALID_EXCHANGES
from utils.logging import get_logger

# Initialize logger
logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# WebSocket-cache-first quotes (broker 429 mitigation)
# ---------------------------------------------------------------------------
# Every tick flowing through the WebSocket proxy already lands in
# MarketDataService. Serving /api/v1/quotes from that cache when fresh — and
# auto-subscribing symbols that miss so their NEXT poll hits the cache —
# removes almost all broker REST quote traffic (the dominant source of Fyers
# 429s; see log/errors.jsonl). Cache misses fall through to the broker REST
# call exactly as before, so behaviour never degrades below the status quo.
# Same pattern the sandbox MTM engine already uses (sandbox/position_manager).
# ---------------------------------------------------------------------------
QUOTES_WS_CACHE_ENABLED = os.getenv("QUOTES_WS_CACHE", "true").lower() == "true"
QUOTES_WS_CACHE_MAX_AGE = float(os.getenv("QUOTES_WS_CACHE_MAX_AGE", "5"))

# Throttle auto-subscribe attempts per symbol so a symbol the feed cannot
# serve (e.g. unsupported exchange) doesn't re-subscribe on every poll.
_ws_autosub_lock = threading.Lock()
_ws_autosub_last: dict[tuple[str, str], float] = {}
_WS_AUTOSUB_RETRY_SECONDS = 300.0


def _quote_from_ws_cache(symbol: str, exchange: str) -> dict[str, Any] | None:
    """
    Serve a quote from the WebSocket-fed MarketDataService cache when fresh.

    Returns a REST-schema quote dict, or None when the symbol has no fresh
    Quote-mode data (not subscribed yet, feed stale, or LTP-only entry).
    Bid/ask come from the depth cache when present, otherwise 0 — consumers
    needing guaranteed depth should subscribe in Depth mode or rely on the
    REST fallback that a cache miss triggers.
    """
    if not QUOTES_WS_CACHE_ENABLED:
        return None
    try:
        from services.market_data_service import get_market_data_service

        data = get_market_data_service().get_all_data(symbol, exchange)
        if not data:
            return None
        if time.time() - data.get("last_update", 0) > QUOTES_WS_CACHE_MAX_AGE:
            return None
        quote = data.get("quote")
        if not quote:  # LTP-only entry — not enough to honour the REST schema
            return None
        ltp = quote.get("ltp", 0)
        if not ltp or ltp <= 0:
            return None
        depth = data.get("depth") or {}
        buy_levels = depth.get("buy") or []
        sell_levels = depth.get("sell") or []
        return {
            "bid": buy_levels[0].get("price", 0) if buy_levels else 0,
            "ask": sell_levels[0].get("price", 0) if sell_levels else 0,
            "open": quote.get("open", 0),
            "high": quote.get("high", 0),
            "low": quote.get("low", 0),
            "ltp": ltp,
            "prev_close": quote.get("close", 0),
            "volume": quote.get("volume", 0),
            "oi": int(quote.get("oi", 0) or 0),
        }
    except Exception as e:
        logger.debug(f"WS quote cache lookup failed for {exchange}:{symbol}: {e}")
        return None


def _autosubscribe_symbols(symbols: list[dict[str, str]]) -> None:
    """
    Subscribe REST-quoted symbols to the WebSocket feed (Quote mode) so
    subsequent polls are served from the tick cache. Fire-and-forget in a
    background thread (green thread under eventlet); throttled per symbol.
    """
    if not QUOTES_WS_CACHE_ENABLED or not symbols:
        return

    now = time.time()
    due = []
    with _ws_autosub_lock:
        for item in symbols:
            key = (item.get("symbol"), item.get("exchange"))
            if not key[0] or not key[1]:
                continue
            if now - _ws_autosub_last.get(key, 0) >= _WS_AUTOSUB_RETRY_SECONDS:
                _ws_autosub_last[key] = now
                due.append({"symbol": key[0], "exchange": key[1]})
    if not due:
        return

    def _subscribe():
        try:
            from database.auth_db import ApiKeys, decrypt_token, get_broker_name
            from services.websocket_service import subscribe_to_symbols

            api_key_obj = ApiKeys.query.first()
            if not api_key_obj:
                return
            api_key = decrypt_token(api_key_obj.api_key_encrypted)
            broker = get_broker_name(api_key) or "unknown"
            success, response, _ = subscribe_to_symbols(
                username=api_key_obj.user_id,
                broker=broker,
                symbols=due,
                mode="Quote",
            )
            if success:
                logger.info(
                    f"Quotes WS cache: auto-subscribed {len(due)} symbol(s): "
                    f"{[s['symbol'] for s in due]}"
                )
            else:
                logger.debug(
                    f"Quotes WS cache: auto-subscribe failed: {response.get('message', response)}"
                )
        except Exception as e:
            logger.debug(f"Quotes WS cache: auto-subscribe error: {e}")

    threading.Thread(target=_subscribe, daemon=True).start()


def validate_symbol_exchange(symbol: str, exchange: str) -> tuple[bool, str | None]:
    """
    Validate that a symbol exists for the given exchange.

    Args:
        symbol: Trading symbol
        exchange: Exchange (e.g., NSE, NFO)

    Returns:
        Tuple of (is_valid, error_message)
    """
    # Validate exchange
    exchange_upper = exchange.upper()
    if exchange_upper not in VALID_EXCHANGES:
        return False, f"Invalid exchange '{exchange}'. Must be one of: {', '.join(VALID_EXCHANGES)}"

    # Validate symbol exists in master contract
    token = get_token(symbol, exchange_upper)
    if token is None:
        return (
            False,
            f"Symbol '{symbol}' not found for exchange '{exchange}'. Please verify the symbol name and ensure master contracts are downloaded.",
        )

    return True, None


def validate_symbols_bulk(
    symbols: list[dict[str, str]],
) -> tuple[bool, list[dict[str, Any]], str | None]:
    """
    Validate multiple symbols and their exchanges.

    Args:
        symbols: List of dicts with 'symbol' and 'exchange' keys

    Returns:
        Tuple of (all_valid, validated_symbols_with_errors, first_error_message)
    """
    all_valid = True
    validated = []
    first_error = None

    for item in symbols:
        symbol = item.get("symbol", "")
        exchange = item.get("exchange", "")

        if not symbol or not exchange:
            error = "Missing symbol or exchange in request"
            validated.append({**item, "valid": False, "error": error})
            if all_valid:
                first_error = error
                all_valid = False
            continue

        is_valid, error = validate_symbol_exchange(symbol, exchange)
        validated.append({**item, "valid": is_valid, "error": error})

        if not is_valid and all_valid:
            first_error = error
            all_valid = False

    return all_valid, validated, first_error


def import_broker_module(broker_name: str) -> Any | None:
    """
    Dynamically import the broker-specific data module.

    Args:
        broker_name: Name of the broker

    Returns:
        The imported module or None if import fails
    """
    try:
        module_path = f"broker.{broker_name}.api.data"
        broker_module = importlib.import_module(module_path)
        return broker_module
    except ImportError as error:
        logger.error(f"Error importing broker module '{module_path}': {error}")
        return None


def get_quotes_with_auth(
    auth_token: str, feed_token: str | None, broker: str, symbol: str, exchange: str
) -> tuple[bool, dict[str, Any], int]:
    """
    Get real-time quotes for a symbol using provided auth tokens.

    Args:
        auth_token: Authentication token for the broker API
        feed_token: Feed token for market data (if required by broker)
        broker: Name of the broker
        symbol: Trading symbol
        exchange: Exchange (e.g., NSE, BSE)

    Returns:
        Tuple containing:
        - Success status (bool)
        - Response data (dict)
        - HTTP status code (int)
    """
    # Validate symbol and exchange before making broker API call
    is_valid, error_msg = validate_symbol_exchange(symbol, exchange)
    if not is_valid:
        return False, {"status": "error", "message": error_msg}, 400

    # WebSocket cache first — a fresh tick answers without any broker call
    ws_quote = _quote_from_ws_cache(symbol, exchange)
    if ws_quote is not None:
        return True, {"status": "success", "data": ws_quote}, 200
    # Miss: subscribe so the next poll hits the cache, then fall through to REST
    _autosubscribe_symbols([{"symbol": symbol, "exchange": exchange}])

    broker_module = import_broker_module(broker)
    if broker_module is None:
        return False, {"status": "error", "message": "Broker-specific module not found"}, 404

    try:
        # Initialize broker's data handler based on broker's requirements
        if hasattr(broker_module.BrokerData.__init__, "__code__"):
            # Check number of parameters the broker's __init__ accepts
            param_count = broker_module.BrokerData.__init__.__code__.co_argcount
            if param_count > 2:  # More than self and auth_token
                data_handler = broker_module.BrokerData(auth_token, feed_token)
            else:
                data_handler = broker_module.BrokerData(auth_token)
        else:
            # Fallback to just auth token if we can't inspect
            data_handler = broker_module.BrokerData(auth_token)

        quotes = data_handler.get_quotes(symbol, exchange)

        if quotes is None:
            return False, {"status": "error", "message": "Failed to fetch quotes"}, 500

        return True, {"status": "success", "data": quotes}, 200
    except Exception as e:
        # Check if this is a permission error
        error_msg = str(e)
        if "permission" in error_msg.lower() or "insufficient" in error_msg.lower():
            # Log at debug level for permission errors (common with personal APIs)
            logger.debug(f"Quote fetch permission denied: {error_msg}")
        else:
            # Log other errors normally
            logger.exception(f"Error in broker_module.get_quotes: {e}")

        return False, {"status": "error", "message": str(e)}, 500


def get_quotes(
    symbol: str,
    exchange: str,
    api_key: str | None = None,
    auth_token: str | None = None,
    feed_token: str | None = None,
    broker: str | None = None,
) -> tuple[bool, dict[str, Any], int]:
    """
    Get real-time quotes for a symbol.
    Supports both API-based authentication and direct internal calls.

    Args:
        symbol: Trading symbol
        exchange: Exchange (e.g., NSE, BSE)
        api_key: OpenAlgo API key (for API-based calls)
        auth_token: Direct broker authentication token (for internal calls)
        feed_token: Direct broker feed token (for internal calls)
        broker: Direct broker name (for internal calls)

    Returns:
        Tuple containing:
        - Success status (bool)
        - Response data (dict)
        - HTTP status code (int)
    """
    # Case 1: API-based authentication
    if api_key and not (auth_token and broker):
        AUTH_TOKEN, FEED_TOKEN, broker_name = get_auth_token_broker(
            api_key, include_feed_token=True
        )
        if AUTH_TOKEN is None:
            return False, {"status": "error", "message": "Invalid openalgo apikey"}, 403
        return get_quotes_with_auth(AUTH_TOKEN, FEED_TOKEN, broker_name, symbol, exchange)

    # Case 2: Direct internal call with auth_token and broker
    elif auth_token and broker:
        return get_quotes_with_auth(auth_token, feed_token, broker, symbol, exchange)

    # Case 3: Invalid parameters
    else:
        return (
            False,
            {
                "status": "error",
                "message": "Either api_key or both auth_token and broker must be provided",
            },
            400,
        )


def get_multiquotes_with_auth(
    auth_token: str, feed_token: str | None, broker: str, symbols: list
) -> tuple[bool, dict[str, Any], int]:
    """
    Get real-time quotes for multiple symbols using provided auth tokens.

    Args:
        auth_token: Authentication token for the broker API
        feed_token: Feed token for market data (if required by broker)
        broker: Name of the broker
        symbols: List of dicts with 'symbol' and 'exchange' keys

    Returns:
        Tuple containing:
        - Success status (bool)
        - Response data (dict)
        - HTTP status code (int)
    """
    # Validate all symbols before making broker API calls
    all_valid, validated_symbols, first_error = validate_symbols_bulk(symbols)

    # Separate valid and invalid symbols
    valid_symbols = [item for item in validated_symbols if item.get("valid", False)]
    invalid_symbols = [item for item in validated_symbols if not item.get("valid", False)]

    # If no valid symbols, return error
    if not valid_symbols:
        return (
            False,
            {
                "status": "error",
                "message": first_error or "No valid symbols provided",
                "invalid_symbols": [
                    {
                        "symbol": s.get("symbol"),
                        "exchange": s.get("exchange"),
                        "error": s.get("error"),
                    }
                    for s in invalid_symbols
                ],
            },
            400,
        )

    # WebSocket cache first — serve fresh symbols without any broker call and
    # auto-subscribe the misses so their next poll hits the cache
    cached_results = []
    remaining_symbols = []
    for item in valid_symbols:
        ws_quote = _quote_from_ws_cache(item["symbol"], item["exchange"])
        if ws_quote is not None:
            cached_results.append(
                {"symbol": item["symbol"], "exchange": item["exchange"], "data": ws_quote}
            )
        else:
            remaining_symbols.append(item)
    if remaining_symbols:
        _autosubscribe_symbols(
            [{"symbol": s["symbol"], "exchange": s["exchange"]} for s in remaining_symbols]
        )
    valid_symbols = remaining_symbols

    # Build results list starting with invalid symbols (marked as errors)
    results = []
    for item in invalid_symbols:
        results.append(
            {
                "symbol": item.get("symbol"),
                "exchange": item.get("exchange"),
                "error": item.get("error"),
            }
        )
    results.extend(cached_results)

    # Everything answered from the WebSocket cache — no broker call needed
    if not valid_symbols:
        return True, {"status": "success", "results": results}, 200

    broker_module = import_broker_module(broker)
    if broker_module is None:
        return False, {"status": "error", "message": "Broker-specific module not found"}, 404

    try:
        # Initialize broker's data handler based on broker's requirements
        if hasattr(broker_module.BrokerData.__init__, "__code__"):
            # Check number of parameters the broker's __init__ accepts
            param_count = broker_module.BrokerData.__init__.__code__.co_argcount
            if param_count > 2:  # More than self and auth_token
                data_handler = broker_module.BrokerData(auth_token, feed_token)
            else:
                data_handler = broker_module.BrokerData(auth_token)
        else:
            # Fallback to just auth token if we can't inspect
            data_handler = broker_module.BrokerData(auth_token)

        # Check if broker supports multiquotes
        if not hasattr(data_handler, "get_multiquotes"):
            # Fallback: fetch quotes one by one for valid symbols only
            logger.debug(
                f"Broker {broker} doesn't support multiquotes, falling back to individual quotes"
            )
            for item in valid_symbols:
                try:
                    quote = data_handler.get_quotes(item["symbol"], item["exchange"])
                    results.append(
                        {"symbol": item["symbol"], "exchange": item["exchange"], "data": quote}
                    )
                except Exception as e:
                    logger.exception(
                        f"Error fetching quote for {item['exchange']}:{item['symbol']}: {e}"
                    )
                    results.append(
                        {"symbol": item["symbol"], "exchange": item["exchange"], "error": str(e)}
                    )

            return True, {"status": "success", "results": results}, 200

        # Use broker's native multiquotes method with only valid symbols
        # Strip validation metadata before passing to broker
        clean_symbols = [{"symbol": s["symbol"], "exchange": s["exchange"]} for s in valid_symbols]
        multiquotes = data_handler.get_multiquotes(clean_symbols)

        if multiquotes is None:
            return False, {"status": "error", "message": "Failed to fetch multiquotes"}, 500

        # Combine broker results with invalid symbol errors
        combined_results = results + (multiquotes if isinstance(multiquotes, list) else [])

        return True, {"status": "success", "results": combined_results}, 200
    except Exception as e:
        # Check if this is a permission error
        error_msg = str(e)
        if "permission" in error_msg.lower() or "insufficient" in error_msg.lower():
            # Log at debug level for permission errors (common with personal APIs)
            logger.debug(f"Multiquote fetch permission denied: {error_msg}")
        else:
            # Log other errors normally
            logger.exception(f"Error in broker_module.get_multiquotes: {e}")

        return False, {"status": "error", "message": str(e)}, 500


def get_multiquotes(
    symbols: list,
    api_key: str | None = None,
    auth_token: str | None = None,
    feed_token: str | None = None,
    broker: str | None = None,
) -> tuple[bool, dict[str, Any], int]:
    """
    Get real-time quotes for multiple symbols.
    Supports both API-based authentication and direct internal calls.

    Args:
        symbols: List of dicts with 'symbol' and 'exchange' keys
        api_key: OpenAlgo API key (for API-based calls)
        auth_token: Direct broker authentication token (for internal calls)
        feed_token: Direct broker feed token (for internal calls)
        broker: Direct broker name (for internal calls)

    Returns:
        Tuple containing:
        - Success status (bool)
        - Response data (dict)
        - HTTP status code (int)
    """
    # Case 1: API-based authentication
    if api_key and not (auth_token and broker):
        AUTH_TOKEN, FEED_TOKEN, broker_name = get_auth_token_broker(
            api_key, include_feed_token=True
        )
        if AUTH_TOKEN is None:
            return False, {"status": "error", "message": "Invalid openalgo apikey"}, 403
        return get_multiquotes_with_auth(AUTH_TOKEN, FEED_TOKEN, broker_name, symbols)

    # Case 2: Direct internal call with auth_token and broker
    elif auth_token and broker:
        return get_multiquotes_with_auth(auth_token, feed_token, broker, symbols)

    # Case 3: Invalid parameters
    else:
        return (
            False,
            {
                "status": "error",
                "message": "Either api_key or both auth_token and broker must be provided",
            },
            400,
        )
