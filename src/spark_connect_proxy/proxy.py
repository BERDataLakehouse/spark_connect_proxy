"""
Transparent gRPC proxy for Spark Connect.

Routes incoming Spark Connect gRPC calls to the correct user's notebook pod
based on the KBase authentication token in the request metadata.

Messages are forwarded as opaque bytes — no proto definitions required.
"""

import logging
from collections.abc import AsyncIterator

import grpc
from grpc import aio

from spark_connect_proxy.auth import AuthError, TokenValidator
from spark_connect_proxy.config import ProxySettings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Spark Connect service method definitions
# ---------------------------------------------------------------------------
# (request_type, response_type) where "unary" or "stream"

_SERVICE = "/spark.connect.SparkConnectService/"

SPARK_CONNECT_METHODS: dict[str, tuple[str, str]] = {
    f"{_SERVICE}ExecutePlan": ("unary", "stream"),
    f"{_SERVICE}AnalyzePlan": ("unary", "unary"),
    f"{_SERVICE}Config": ("unary", "unary"),
    f"{_SERVICE}AddArtifacts": ("stream", "unary"),
    f"{_SERVICE}ArtifactStatus": ("unary", "unary"),
    f"{_SERVICE}Interrupt": ("unary", "unary"),
    f"{_SERVICE}ReattachExecute": ("unary", "stream"),
    f"{_SERVICE}ReleaseExecute": ("unary", "unary"),
    f"{_SERVICE}FetchErrorDetails": ("unary", "unary"),
}

# Identity serializers — pass raw bytes through without parsing
_IDENTITY = lambda x: x  # noqa: E731


def _extract_token(metadata: tuple[tuple[str, str | bytes], ...] | None) -> str:
    """Extract the x-kbase-token from gRPC invocation metadata."""
    if metadata:
        for key, value in metadata:
            if key == "x-kbase-token":
                return value.decode() if isinstance(value, bytes) else value
    raise AuthError("Missing x-kbase-token in request metadata")


# ---------------------------------------------------------------------------
# Channel pool — reuse channels to the same backend
# ---------------------------------------------------------------------------


class ChannelPool:
    """Manages a pool of gRPC channels to backend Spark Connect servers."""

    def __init__(self) -> None:
        self._channels: dict[str, aio.Channel] = {}

    def get_channel(self, target: str) -> aio.Channel:
        """Get or create a channel to the specified backend target."""
        if target not in self._channels:
            logger.info("Opening channel to backend: %s", target)
            self._channels[target] = aio.insecure_channel(target)
        return self._channels[target]

    async def close_all(self) -> None:
        """Close all open channels."""
        for target, channel in self._channels.items():
            logger.info("Closing channel to: %s", target)
            await channel.close()
        self._channels.clear()


# ---------------------------------------------------------------------------
# Proxy method handlers
# ---------------------------------------------------------------------------


async def _proxy_unary_unary(
    method: str,
    request: bytes,
    context: aio.ServicerContext,
    channel: aio.Channel,
    metadata: tuple[tuple[str, str | bytes], ...],
) -> bytes:
    """Proxy a unary-unary RPC."""
    try:
        response = await channel.unary_unary(
            method,
            request_serializer=_IDENTITY,
            response_deserializer=_IDENTITY,
        )(request, metadata=metadata)
        return response
    except aio.AioRpcError as e:
        await context.abort(e.code(), e.details() or "Backend error")
        return b""  # unreachable


async def _proxy_unary_stream(
    method: str,
    request: bytes,
    context: aio.ServicerContext,
    channel: aio.Channel,
    metadata: tuple[tuple[str, str | bytes], ...],
) -> AsyncIterator[bytes]:
    """Proxy a unary-stream (server streaming) RPC."""
    try:
        call = channel.unary_stream(
            method,
            request_serializer=_IDENTITY,
            response_deserializer=_IDENTITY,
        )(request, metadata=metadata)
        async for response in call:
            yield response
    except aio.AioRpcError as e:
        await context.abort(e.code(), e.details() or "Backend error")


async def _proxy_stream_unary(
    method: str,
    request_iterator: AsyncIterator[bytes],
    context: aio.ServicerContext,
    channel: aio.Channel,
    metadata: tuple[tuple[str, str | bytes], ...],
) -> bytes:
    """Proxy a stream-unary (client streaming) RPC."""
    try:
        response = await channel.stream_unary(
            method,
            request_serializer=_IDENTITY,
            response_deserializer=_IDENTITY,
        )(request_iterator, metadata=metadata)
        return response
    except aio.AioRpcError as e:
        await context.abort(e.code(), e.details() or "Backend error")
        return b""  # unreachable


# ---------------------------------------------------------------------------
# Generic RPC handler
# ---------------------------------------------------------------------------


class SparkConnectProxyHandler(grpc.GenericRpcHandler):
    """
    Generic gRPC handler that intercepts all Spark Connect RPCs and proxies
    them to the correct user's backend based on KBase token authentication.
    """

    def __init__(self, settings: ProxySettings, validator: TokenValidator, pool: ChannelPool):
        self._settings = settings
        self._validator = validator
        self._pool = pool

    def service(
        self, handler_call_details: grpc.HandlerCallDetails
    ) -> grpc.RpcMethodHandler | None:
        method = handler_call_details.method

        if method not in SPARK_CONNECT_METHODS:
            return None

        req_type, resp_type = SPARK_CONNECT_METHODS[method]
        metadata = handler_call_details.invocation_metadata

        # Authenticate and resolve backend target
        try:
            token = _extract_token(metadata)
            username = self._validator.get_username(token)
        except AuthError as e:
            logger.warning("Authentication failed: %s", e)
            return self._unauthenticated_handler(str(e), req_type, resp_type)

        target = self._settings.backend_target(username)
        channel = self._pool.get_channel(target)

        # Forward original metadata (including x-kbase-token for server-side validation)
        fwd_metadata: tuple[tuple[str, str | bytes], ...] = tuple(metadata) if metadata else ()

        logger.debug("Proxying %s for user %s → %s", method, username, target)

        # Return the appropriate handler type
        # Return the appropriate handler type
        if req_type == "unary" and resp_type == "unary":

            async def unary_unary_behavior(request, context):
                return await _proxy_unary_unary(method, request, context, channel, fwd_metadata)

            return grpc.unary_unary_rpc_method_handler(
                unary_unary_behavior,
                request_deserializer=_IDENTITY,
                response_serializer=_IDENTITY,
            )
        elif req_type == "unary" and resp_type == "stream":

            async def unary_stream_behavior(request, context):
                async for response in _proxy_unary_stream(
                    method, request, context, channel, fwd_metadata
                ):
                    yield response

            return grpc.unary_stream_rpc_method_handler(
                unary_stream_behavior,
                request_deserializer=_IDENTITY,
                response_serializer=_IDENTITY,
            )
        elif req_type == "stream" and resp_type == "unary":

            async def stream_unary_behavior(request_iterator, context):
                return await _proxy_stream_unary(
                    method, request_iterator, context, channel, fwd_metadata
                )

            return grpc.stream_unary_rpc_method_handler(
                stream_unary_behavior,
                request_deserializer=_IDENTITY,
                response_serializer=_IDENTITY,
            )
        else:
            return None

    def _unauthenticated_handler(
        self, message: str, req_type: str, resp_type: str
    ) -> grpc.RpcMethodHandler:
        """Return a handler that immediately aborts with UNAUTHENTICATED."""

        async def _abort_unary(_request: bytes, context: aio.ServicerContext) -> bytes:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, message)
            return b""  # unreachable but satisfies type checker

        async def _abort_stream(
            _request: bytes, context: aio.ServicerContext
        ) -> AsyncIterator[bytes]:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, message)
            return  # type: ignore[return-value]
            yield  # noqa: F841 — unreachable, makes this a generator

        async def _abort_client_stream(
            _request_iterator: AsyncIterator[bytes], context: aio.ServicerContext
        ) -> bytes:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, message)
            return b""

        if req_type == "unary" and resp_type == "unary":
            return grpc.unary_unary_rpc_method_handler(
                _abort_unary,
                request_deserializer=_IDENTITY,
                response_serializer=_IDENTITY,
            )
        elif req_type == "unary" and resp_type == "stream":
            return grpc.unary_stream_rpc_method_handler(
                _abort_stream,
                request_deserializer=_IDENTITY,
                response_serializer=_IDENTITY,
            )
        elif req_type == "stream" and resp_type == "unary":
            return grpc.stream_unary_rpc_method_handler(
                _abort_client_stream,
                request_deserializer=_IDENTITY,
                response_serializer=_IDENTITY,
            )
        else:
            return grpc.unary_unary_rpc_method_handler(
                _abort_unary,
                request_deserializer=_IDENTITY,
                response_serializer=_IDENTITY,
            )


# ---------------------------------------------------------------------------
# Server lifecycle
# ---------------------------------------------------------------------------


async def serve(settings: ProxySettings | None = None) -> None:
    """Start the gRPC proxy server."""
    if settings is None:
        settings = ProxySettings()

    validator = TokenValidator(
        auth_url=settings.KBASE_AUTH_URL,
        cache_ttl=settings.TOKEN_CACHE_TTL,
        cache_max_size=settings.TOKEN_CACHE_MAX_SIZE,
        require_mfa=settings.REQUIRE_MFA,
    )
    pool = ChannelPool()
    handler = SparkConnectProxyHandler(settings, validator, pool)

    server = aio.server()
    server.add_generic_rpc_handlers([handler])
    listen_addr = f"[::]:{settings.PROXY_LISTEN_PORT}"
    server.add_insecure_port(listen_addr)

    logger.info("Spark Connect Proxy starting on %s", listen_addr)
    logger.info("Backend template: %s:%d", settings.SERVICE_TEMPLATE, settings.BACKEND_PORT)

    await server.start()

    try:
        await server.wait_for_termination()
    finally:
        logger.info("Shutting down proxy server...")
        await pool.close_all()
        await server.stop(grace=5)
        logger.info("Proxy server stopped.")
