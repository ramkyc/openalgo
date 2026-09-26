import logging
from collections.abc import Callable, Iterable
from functools import wraps
from typing import Any

from flask import Flask, abort, jsonify, request
from werkzeug.exceptions import Forbidden

from database.traffic_db import Error404Tracker, IPBan, logs_session
from utils.ip_helper import get_real_ip, get_real_ip_from_environ

logger = logging.getLogger(__name__)

# WSGI types
WSGIEnviron = dict[str, Any]
StartResponse = Callable[..., Any]
WSGIApp = Callable[[WSGIEnviron, StartResponse], Iterable[bytes]]


class SecurityMiddleware:
    """WSGI middleware that blocks requests from banned IP addresses.

    This middleware sits at the WSGI layer, intercepting requests before they
    reach Flask. Banned IPs receive a 403 Forbidden response immediately,
    bypassing all application logic.

    Attributes:
        app: The wrapped WSGI application to delegate non-banned requests to.
    """

    def __init__(self, app: WSGIApp) -> None:
        """Initialize the security middleware.

        Args:
            app: The WSGI application to wrap.
        """
        self.app = app

    def __call__(
        self, environ: WSGIEnviron, start_response: StartResponse
    ) -> Iterable[bytes]:
        """Process an incoming WSGI request.

        Checks the client IP against the ban list. Banned IPs receive a
        403 Forbidden response. All other requests are passed through to
        the wrapped application.

        Args:
            environ: The WSGI environment dictionary.
            start_response: The WSGI start_response callable.

        Returns:
            The WSGI response iterable.
        """
        # Get real client IP (handles proxies)
        client_ip = get_real_ip_from_environ(environ)

        # Check if IP is banned — this opens a logs_session connection.
        # Must clean up in ALL paths (banned and non-banned) because this
        # runs at WSGI level, outside Flask's teardown_appcontext scope.
        try:
            is_banned = IPBan.is_ip_banned(client_ip)
        finally:
            logs_session.remove()

        if is_banned:
            # Return 403 Forbidden for banned IPs
            status = "403 Forbidden"
            headers = [("Content-Type", "text/plain")]
            start_response(status, headers)
            logger.warning(f"Blocked banned IP: {client_ip}")
            return [b"Access Denied: Your IP has been banned"]

        try:
            return self.app(environ, start_response)
        except KeyError as e:
            # engineio's handle_request() guards this exact race for GET
            # (try/except around _get_socket) but not for POST: a client can
            # disconnect between the "sid in self.sockets" check and the
            # dereference a few lines later, on another thread under
            # async_mode="threading". POST then raises an uncaught
            # KeyError('Session is disconnected') instead of the clean 400
            # GET already returns for the same situation -- upstream fix
            # https://github.com/miguelgrinberg/python-engineio/commit/55a9e46
            # applied it to GET only, still true as of engineio 4.13.5. This
            # mirrors that fix at the WSGI boundary rather than patching
            # vendored code. Harmless: the socket.io client already treats a
            # disconnected-session response as a signal to reconnect.
            if str(e) != "'Session is disconnected'":
                raise
            logger.debug(f"Socket.IO session race on {environ.get('PATH_INFO')}: {e}")
            status = "400 BAD REQUEST"
            headers = [("Content-Type", "text/plain")]
            start_response(status, headers)
            return [b'"Session is disconnected"']


def check_ip_ban(f: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator that aborts with 403 if the requesting IP is banned.

    Use this on individual Flask route handlers to enforce IP bans at the
    application level, complementing the WSGI-level SecurityMiddleware.

    Args:
        f: The Flask view function to wrap.

    Returns:
        The wrapped function that checks the IP ban list before proceeding.
    """

    @wraps(f)
    def decorated_function(*args: Any, **kwargs: Any) -> Any:
        client_ip = get_real_ip()

        if IPBan.is_ip_banned(client_ip):
            logger.warning(f"Blocked banned IP in decorator: {client_ip}")
            abort(403, description="Access Denied: Your IP has been banned")

        return f(*args, **kwargs)

    return decorated_function


def init_security_middleware(app: Flask) -> None:
    """Initialize security middleware and error handlers on the Flask app.

    Wraps the Flask WSGI application with ``SecurityMiddleware`` to block
    banned IPs at the WSGI layer, and registers a 403 error handler that
    returns a JSON response.

    Args:
        app: The Flask application instance to configure.
    """
    # Wrap the WSGI app with security middleware
    app.wsgi_app = SecurityMiddleware(app.wsgi_app)

    logger.debug("Security middleware initialized")

    # Note: 404 handler is now in app.py to avoid conflicts
    # The main app's 404 handler calls Error404Tracker.track_404()

    # Register 403 error handler for banned IPs
    @app.errorhandler(403)
    def handle_403(e: Forbidden) -> tuple[Any, int]:
        """Return a JSON error response for 403 Forbidden.

        Args:
            e: The Forbidden exception raised by Flask.

        Returns:
            A tuple of the JSON response body and 403 status code.
        """
        return jsonify({"error": "Access Denied"}), 403

    logger.debug("Security middleware initialized")
