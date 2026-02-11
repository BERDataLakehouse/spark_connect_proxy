"""
Transparent gRPC proxy for Spark Connect.

Routes incoming Spark Connect gRPC calls to the correct user's notebook pod
based on the KBase authentication token in the request metadata.

Messages are forwarded as opaque bytes — no proto definitions required.
"""

import asyncio
import logging
import time
from collections.abc import AsyncIterator

import grpc
from grpc import aio
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

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
# Channel pool — reuse channels to the same backend with LRU eviction
# ---------------------------------------------------------------------------


class ChannelPool:
    """Manages a pool of gRPC channels to backend Spark Connect servers.

    Tracks last-used time for each channel and evicts least-recently-used
    entries when the pool exceeds max_size.
    """

    def __init__(self, max_size: int = 100) -> None:
        self._max_size = max_size
        # target → (channel, last_used_timestamp)
        self._channels: dict[str, tuple[aio.Channel, float]] = {}

    def get_channel(self, target: str) -> aio.Channel:
        """Get or create a channel to the specified backend target."""
        if target in self._channels:
            channel, _ = self._channels[target]
            self._channels[target] = (channel, time.monotonic())
            return channel

        # Evict LRU if at capacity
        if len(self._channels) >= self._max_size:
            self._evict_lru()

        logger.info("Opening channel to backend: %s", target)
        channel = aio.insecure_channel(target)
        self._channels[target] = (channel, time.monotonic())
        return channel

    def _evict_lru(self) -> None:
        """Evict the least-recently-used channel."""
        if not self._channels:
            return
        lru_target = min(self._channels, key=lambda t: self._channels[t][1])
        channel, _ = self._channels.pop(lru_target)
        logger.info("Evicting idle channel to: %s", lru_target)
        # Close asynchronously — fire and forget
        asyncio.ensure_future(channel.close())

    async def close_all(self) -> None:
        """Close all open channels."""
        for target, (channel, _) in self._channels.items():
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
# Generic RPC handler — authentication is deferred to async context
# ---------------------------------------------------------------------------


class SparkConnectProxyHandler(grpc.GenericRpcHandler):
    """
    Generic gRPC handler that intercepts all Spark Connect RPCs and proxies
    them to the correct user's backend based on KBase token authentication.

    Authentication is performed inside the async handler behaviors (not in
    the synchronous service() method) to avoid blocking the event loop.
    """

    def __init__(self, settings: ProxySettings, validator: TokenValidator, pool: ChannelPool):
        self._settings = settings
        self._validator = validator
        self._pool = pool

    async def _authenticate(
        self, metadata: tuple[tuple[str, str | bytes], ...] | None, context: aio.ServicerContext
    ) -> tuple[aio.Channel, tuple[tuple[str, str | bytes], ...]]:
        """Authenticate and resolve backend — runs in async context.

        Returns:
            (channel, forward_metadata) tuple on success.

        Raises:
            Aborts the gRPC context with UNAUTHENTICATED on failure.
        """
        try:
            token = _extract_token(metadata)
            # Run the synchronous auth call in a thread to avoid blocking
            username = await asyncio.to_thread(self._validator.get_username, token)
        except AuthError as e:
            logger.warning("Authentication failed: %s", e)
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, str(e))
            raise  # unreachable after abort, satisfies type checker

        target = self._settings.backend_target(username)
        channel = self._pool.get_channel(target)
        fwd_metadata: tuple[tuple[str, str | bytes], ...] = tuple(metadata) if metadata else ()

        logger.debug("Proxying for user %s → %s", username, target)
        return channel, fwd_metadata

    def service(
        self, handler_call_details: grpc.HandlerCallDetails
    ) -> grpc.RpcMethodHandler | None:
        method = handler_call_details.method

        if method not in SPARK_CONNECT_METHODS:
            return None

        req_type, resp_type = SPARK_CONNECT_METHODS[method]
        metadata = handler_call_details.invocation_metadata

        # Return the appropriate handler type
        # Authentication is deferred to the async behavior function
        if req_type == "unary" and resp_type == "unary":

            async def unary_unary_behavior(request: bytes, context: aio.ServicerContext) -> bytes:
                channel, fwd_metadata = await self._authenticate(metadata, context)
                return await _proxy_unary_unary(method, request, context, channel, fwd_metadata)

            return grpc.unary_unary_rpc_method_handler(
                unary_unary_behavior,
                request_deserializer=_IDENTITY,
                response_serializer=_IDENTITY,
            )
        elif req_type == "unary" and resp_type == "stream":

            async def unary_stream_behavior(
                request: bytes, context: aio.ServicerContext
            ) -> AsyncIterator[bytes]:
                channel, fwd_metadata = await self._authenticate(metadata, context)
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

            async def stream_unary_behavior(
                request_iterator: AsyncIterator[bytes], context: aio.ServicerContext
            ) -> bytes:
                channel, fwd_metadata = await self._authenticate(metadata, context)
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
    pool = ChannelPool(max_size=settings.MAX_CHANNELS_PER_BACKEND)
    handler = SparkConnectProxyHandler(settings, validator, pool)

    server = aio.server()
    server.add_generic_rpc_handlers([handler])

    # Register gRPC health check service
    health_servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    # Mark the proxy as serving
    await health_servicer.set(
        "spark.connect.SparkConnectService",
        health_pb2.HealthCheckResponse.SERVING,
    )
    # Also set the overall server health
    await health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)

    listen_addr = f"[::]:{settings.PROXY_LISTEN_PORT}"
    server.add_insecure_port(listen_addr)

    logger.info("Spark Connect Proxy starting on %s", listen_addr)
    logger.info("Backend template: %s:%d", settings.SERVICE_TEMPLATE, settings.BACKEND_PORT)

    await server.start()

    try:
        await server.wait_for_termination()
    finally:
        logger.info("Shutting down proxy server...")
        # Mark as not serving before closing
        await health_servicer.set(
            "spark.connect.SparkConnectService",
            health_pb2.HealthCheckResponse.NOT_SERVING,
        )
        await health_servicer.set("", health_pb2.HealthCheckResponse.NOT_SERVING)
        await pool.close_all()
        await server.stop(grace=5)
        logger.info("Proxy server stopped.")
