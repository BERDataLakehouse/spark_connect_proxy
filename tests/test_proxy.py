"""Tests for the gRPC proxy handler."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest

from spark_connect_proxy.auth import AuthError, TokenValidator
from spark_connect_proxy.config import ProxySettings
from spark_connect_proxy.proxy import (
    ChannelPool,
    SparkConnectProxyHandler,
    _extract_token,
    _proxy_stream_unary,
    _proxy_unary_stream,
    _proxy_unary_unary,
    serve,
)

# ---------------------------------------------------------------------------
# _extract_token tests
# ---------------------------------------------------------------------------


class TestExtractToken:
    """Tests for _extract_token()."""

    def test_extracts_token(self) -> None:
        metadata = (("x-kbase-token", "my-token"), ("other", "value"))
        assert _extract_token(metadata) == "my-token"

    def test_extracts_bytes_token(self) -> None:
        """Bytes-valued token is decoded to str."""
        metadata = (("x-kbase-token", b"my-bytes-token"),)
        assert _extract_token(metadata) == "my-bytes-token"

    def test_missing_token(self) -> None:
        metadata = (("other-header", "value"),)
        with pytest.raises(AuthError, match="Missing x-kbase-token"):
            _extract_token(metadata)

    def test_none_metadata(self) -> None:
        with pytest.raises(AuthError, match="Missing x-kbase-token"):
            _extract_token(None)

    def test_empty_metadata(self) -> None:
        with pytest.raises(AuthError, match="Missing x-kbase-token"):
            _extract_token(())


# ---------------------------------------------------------------------------
# ChannelPool tests
# ---------------------------------------------------------------------------


class TestChannelPool:
    """Tests for ChannelPool with LRU eviction."""

    def test_reuses_channels(self) -> None:
        pool = ChannelPool()
        ch1 = pool.get_channel("target1:15002")
        ch2 = pool.get_channel("target1:15002")
        assert ch1 is ch2

    def test_separate_channels_per_target(self) -> None:
        pool = ChannelPool()
        ch1 = pool.get_channel("target1:15002")
        ch2 = pool.get_channel("target2:15002")
        assert ch1 is not ch2

    @pytest.mark.asyncio
    async def test_close_all(self) -> None:
        """close_all() closes all channels and clears the pool."""
        pool = ChannelPool()
        ch1 = pool.get_channel("target1:15002")
        ch2 = pool.get_channel("target2:15002")
        assert len(pool._channels) == 2

        # Mock the close method on each channel
        ch1.close = AsyncMock()
        ch2.close = AsyncMock()

        await pool.close_all()
        ch1.close.assert_awaited_once()
        ch2.close.assert_awaited_once()
        assert len(pool._channels) == 0

    def test_lru_eviction(self) -> None:
        """Evicts least-recently-used channel when pool is full."""
        pool = ChannelPool(max_size=2)

        pool.get_channel("target1:15002")
        pool.get_channel("target2:15002")
        assert len(pool._channels) == 2

        # Access target1 to make target2 the LRU
        pool.get_channel("target1:15002")

        # Adding target3 should evict target2 (LRU)
        with patch("spark_connect_proxy.proxy.asyncio.ensure_future"):
            pool.get_channel("target3:15002")

        assert len(pool._channels) == 2
        assert "target1:15002" in pool._channels
        assert "target3:15002" in pool._channels
        assert "target2:15002" not in pool._channels

    def test_updates_last_used_on_access(self) -> None:
        """Accessing an existing channel updates its last-used timestamp."""
        pool = ChannelPool()
        pool.get_channel("target1:15002")
        _, ts1 = pool._channels["target1:15002"]

        import time

        time.sleep(0.01)
        pool.get_channel("target1:15002")
        _, ts2 = pool._channels["target1:15002"]

        assert ts2 > ts1


# ---------------------------------------------------------------------------
# Proxy function tests
# ---------------------------------------------------------------------------


class TestProxyFunctions:
    """Tests for _proxy_unary_unary, _proxy_unary_stream, _proxy_stream_unary."""

    @pytest.mark.asyncio
    async def test_proxy_unary_unary(self) -> None:
        """_proxy_unary_unary forwards request and returns response."""
        mock_channel = MagicMock()
        mock_call = AsyncMock(return_value=b"response-bytes")
        mock_channel.unary_unary.return_value = mock_call

        context = MagicMock()
        result = await _proxy_unary_unary(
            "/test/Method", b"request-bytes", context, mock_channel, (("key", "val"),)
        )

        assert result == b"response-bytes"
        mock_channel.unary_unary.assert_called_once()
        mock_call.assert_awaited_once_with(b"request-bytes", metadata=(("key", "val"),))

    @pytest.mark.asyncio
    async def test_proxy_unary_stream(self) -> None:
        """_proxy_unary_stream yields all responses from the backend."""
        mock_channel = MagicMock()

        async def mock_stream(*_args, **_kwargs):
            for chunk in [b"chunk-1", b"chunk-2", b"chunk-3"]:
                yield chunk

        mock_call = MagicMock()
        mock_call.return_value = mock_stream()
        mock_channel.unary_stream.return_value = mock_call

        context = MagicMock()
        results = []
        async for response in _proxy_unary_stream(
            "/test/Method", b"request", context, mock_channel, ()
        ):
            results.append(response)

        assert results == [b"chunk-1", b"chunk-2", b"chunk-3"]

    @pytest.mark.asyncio
    async def test_proxy_stream_unary(self) -> None:
        """_proxy_stream_unary forwards request iterator and returns response."""
        mock_channel = MagicMock()
        mock_call = AsyncMock(return_value=b"stream-response")
        mock_channel.stream_unary.return_value = mock_call

        async def request_iter():
            yield b"part-1"
            yield b"part-2"

        context = MagicMock()
        result = await _proxy_stream_unary(
            "/test/Method", request_iter(), context, mock_channel, (("k", "v"),)
        )

        assert result == b"stream-response"
        mock_channel.stream_unary.assert_called_once()


# ---------------------------------------------------------------------------
# SparkConnectProxyHandler tests
# ---------------------------------------------------------------------------


class TestSparkConnectProxyHandler:
    """Tests for SparkConnectProxyHandler routing logic."""

    def setup_method(self) -> None:
        self.settings = ProxySettings(
            BACKEND_NAMESPACE="test-ns",
            SERVICE_TEMPLATE="jupyter-{username}.{namespace}.svc.local",
        )
        self.validator = MagicMock(spec=TokenValidator)
        self.pool = MagicMock(spec=ChannelPool)
        self.handler = SparkConnectProxyHandler(self.settings, self.validator, self.pool)

    def _make_call_details(
        self, method: str, metadata: tuple[tuple[str, str], ...] | None = None
    ) -> grpc.HandlerCallDetails:
        details = MagicMock(spec=grpc.HandlerCallDetails)
        details.method = method
        details.invocation_metadata = metadata
        return details

    def test_unknown_method_returns_none(self) -> None:
        """Unknown gRPC methods are rejected (returns None)."""
        details = self._make_call_details("/unknown.Service/Method")
        result = self.handler.service(details)
        assert result is None

    def test_valid_unary_unary_returns_handler(self) -> None:
        """Valid unary-unary method returns a handler (auth deferred to async)."""
        metadata = (("x-kbase-token", "valid"),)
        details = self._make_call_details(
            "/spark.connect.SparkConnectService/AnalyzePlan", metadata
        )
        result = self.handler.service(details)
        assert result is not None
        # Auth should NOT be called in service() — it's deferred
        self.validator.get_username.assert_not_called()

    def test_valid_unary_stream_returns_handler(self) -> None:
        """Valid unary-stream method returns a handler."""
        metadata = (("x-kbase-token", "valid"),)
        details = self._make_call_details(
            "/spark.connect.SparkConnectService/ExecutePlan", metadata
        )
        result = self.handler.service(details)
        assert result is not None

    def test_valid_stream_unary_returns_handler(self) -> None:
        """Valid stream-unary method returns a handler."""
        metadata = (("x-kbase-token", "valid"),)
        details = self._make_call_details(
            "/spark.connect.SparkConnectService/AddArtifacts", metadata
        )
        result = self.handler.service(details)
        assert result is not None

    def test_no_metadata_still_returns_handler(self) -> None:
        """Missing metadata still returns a handler — auth failure happens in async."""
        details = self._make_call_details("/spark.connect.SparkConnectService/Config", None)
        result = self.handler.service(details)
        # Handler is returned; auth will fail when invoked asynchronously
        assert result is not None


# ---------------------------------------------------------------------------
# Async authentication tests
# ---------------------------------------------------------------------------


class TestAuthenticate:
    """Tests for the async _authenticate method."""

    def setup_method(self) -> None:
        self.settings = ProxySettings(
            BACKEND_NAMESPACE="test-ns",
            SERVICE_TEMPLATE="jupyter-{username}.{namespace}.svc.local",
        )
        self.validator = MagicMock(spec=TokenValidator)
        self.pool = ChannelPool()
        self.handler = SparkConnectProxyHandler(self.settings, self.validator, self.pool)

    @pytest.mark.asyncio
    async def test_authenticate_success(self) -> None:
        """Successful auth returns channel and forward metadata."""
        self.validator.get_username.return_value = "alice"
        metadata = (("x-kbase-token", "valid-token"),)
        context = AsyncMock()

        channel, fwd_metadata = await self.handler._authenticate(metadata, context)

        assert channel is not None
        assert ("x-kbase-token", "valid-token") in fwd_metadata
        context.abort.assert_not_called()

    @pytest.mark.asyncio
    async def test_authenticate_routes_correctly(self) -> None:
        """Auth resolves to correct backend target."""
        self.validator.get_username.return_value = "tgu2"
        metadata = (("x-kbase-token", "tok"),)
        context = AsyncMock()

        channel, _ = await self.handler._authenticate(metadata, context)

        # Should have created a channel for the user's backend
        assert "jupyter-tgu2.test-ns.svc.local:15002" in self.pool._channels

    @pytest.mark.asyncio
    async def test_authenticate_missing_token(self) -> None:
        """Missing token aborts with UNAUTHENTICATED."""
        metadata = (("other-header", "value"),)
        context = AsyncMock()
        context.abort = AsyncMock(side_effect=grpc.RpcError())

        with pytest.raises(grpc.RpcError):
            await self.handler._authenticate(metadata, context)

        context.abort.assert_awaited_once()
        args = context.abort.call_args[0]
        assert args[0] == grpc.StatusCode.UNAUTHENTICATED

    @pytest.mark.asyncio
    async def test_authenticate_invalid_token(self) -> None:
        """Invalid token aborts with UNAUTHENTICATED."""
        self.validator.get_username.side_effect = AuthError("Invalid token")
        metadata = (("x-kbase-token", "bad"),)
        context = AsyncMock()
        context.abort = AsyncMock(side_effect=grpc.RpcError())

        with pytest.raises(grpc.RpcError):
            await self.handler._authenticate(metadata, context)

        context.abort.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_authenticate_different_users(self) -> None:
        """Different tokens route to different backends."""
        context = AsyncMock()

        self.validator.get_username.return_value = "tgu2"
        await self.handler._authenticate((("x-kbase-token", "tok1"),), context)

        self.validator.get_username.return_value = "bsadkhin"
        await self.handler._authenticate((("x-kbase-token", "tok2"),), context)

        assert "jupyter-tgu2.test-ns.svc.local:15002" in self.pool._channels
        assert "jupyter-bsadkhin.test-ns.svc.local:15002" in self.pool._channels


# ---------------------------------------------------------------------------
# Handler behavior invocation tests
# ---------------------------------------------------------------------------


class TestHandlerBehaviors:
    """Tests that invoke the async handler behaviors returned by service()."""

    def setup_method(self) -> None:
        self.settings = ProxySettings(
            BACKEND_NAMESPACE="test-ns",
            SERVICE_TEMPLATE="jupyter-{username}.{namespace}.svc.local",
        )
        self.validator = MagicMock(spec=TokenValidator)
        self.validator.get_username.return_value = "testuser"
        self.pool = ChannelPool()
        self.handler = SparkConnectProxyHandler(self.settings, self.validator, self.pool)

    def _make_call_details(
        self, method: str, metadata: tuple[tuple[str, str], ...]
    ) -> grpc.HandlerCallDetails:
        details = MagicMock(spec=grpc.HandlerCallDetails)
        details.method = method
        details.invocation_metadata = metadata
        return details

    @pytest.mark.asyncio
    async def test_unary_unary_behavior(self) -> None:
        """The unary-unary handler authenticates and proxies."""
        metadata = (("x-kbase-token", "tok"),)
        details = self._make_call_details("/spark.connect.SparkConnectService/Config", metadata)
        result = self.handler.service(details)
        assert result is not None

        with patch("spark_connect_proxy.proxy._proxy_unary_unary", new_callable=AsyncMock) as mock:
            mock.return_value = b"response"
            context = AsyncMock()
            response = await result.unary_unary(b"request", context)
            assert response == b"response"
            mock.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_unary_stream_behavior(self) -> None:
        """The unary-stream handler authenticates and proxies."""
        metadata = (("x-kbase-token", "tok"),)
        details = self._make_call_details(
            "/spark.connect.SparkConnectService/ExecutePlan", metadata
        )
        result = self.handler.service(details)
        assert result is not None

        async def mock_proxy(*_args, **_kwargs):
            yield b"chunk-1"
            yield b"chunk-2"

        with patch("spark_connect_proxy.proxy._proxy_unary_stream", side_effect=mock_proxy):
            context = AsyncMock()
            chunks = []
            async for chunk in result.unary_stream(b"request", context):
                chunks.append(chunk)
            assert chunks == [b"chunk-1", b"chunk-2"]

    @pytest.mark.asyncio
    async def test_stream_unary_behavior(self) -> None:
        """The stream-unary handler authenticates and proxies."""
        metadata = (("x-kbase-token", "tok"),)
        details = self._make_call_details(
            "/spark.connect.SparkConnectService/AddArtifacts", metadata
        )
        result = self.handler.service(details)
        assert result is not None

        with patch("spark_connect_proxy.proxy._proxy_stream_unary", new_callable=AsyncMock) as mock:
            mock.return_value = b"aggregated"

            async def request_iter():
                yield b"part"

            context = AsyncMock()
            response = await result.stream_unary(request_iter(), context)
            assert response == b"aggregated"


# ---------------------------------------------------------------------------
# serve() function tests
# ---------------------------------------------------------------------------


class TestServe:
    """Tests for the serve() entry point."""

    @pytest.mark.asyncio
    async def test_serve_starts_and_stops(self) -> None:
        """serve() creates a server, starts it, and can be shut down."""
        mock_server = AsyncMock()
        mock_server.wait_for_termination = AsyncMock(side_effect=asyncio.CancelledError)
        # Make sync methods return non-coroutines
        mock_server.add_generic_rpc_handlers = MagicMock()
        mock_server.add_insecure_port = MagicMock()

        with (
            patch("spark_connect_proxy.proxy.aio.server", return_value=mock_server),
            patch("spark_connect_proxy.proxy.TokenValidator"),
            patch("spark_connect_proxy.proxy.ChannelPool") as mock_pool_cls,
            patch("spark_connect_proxy.proxy.health.HealthServicer") as mock_health_cls,
            patch("spark_connect_proxy.proxy.health_pb2_grpc.add_HealthServicer_to_server"),
        ):
            mock_pool = MagicMock()
            mock_pool.close_all = AsyncMock()
            mock_pool_cls.return_value = mock_pool

            mock_health = AsyncMock()
            mock_health_cls.return_value = mock_health

            settings = ProxySettings()

            with pytest.raises(asyncio.CancelledError):
                await serve(settings)

            mock_server.add_generic_rpc_handlers.assert_called_once()
            mock_server.add_insecure_port.assert_called_once()
            mock_server.start.assert_awaited_once()
            mock_pool.close_all.assert_awaited_once()
            mock_server.stop.assert_awaited_once_with(grace=5)

    @pytest.mark.asyncio
    async def test_serve_uses_default_settings(self) -> None:
        """serve() creates default ProxySettings when none provided."""
        mock_server = AsyncMock()
        mock_server.wait_for_termination = AsyncMock(side_effect=asyncio.CancelledError)
        mock_server.add_generic_rpc_handlers = MagicMock()
        mock_server.add_insecure_port = MagicMock()

        with (
            patch("spark_connect_proxy.proxy.aio.server", return_value=mock_server),
            patch("spark_connect_proxy.proxy.TokenValidator") as mock_validator_cls,
            patch("spark_connect_proxy.proxy.ChannelPool") as mock_pool_cls,
            patch("spark_connect_proxy.proxy.health.HealthServicer") as mock_health_cls,
            patch("spark_connect_proxy.proxy.health_pb2_grpc.add_HealthServicer_to_server"),
        ):
            mock_pool = MagicMock()
            mock_pool.close_all = AsyncMock()
            mock_pool_cls.return_value = mock_pool

            mock_health = AsyncMock()
            mock_health_cls.return_value = mock_health

            with pytest.raises(asyncio.CancelledError):
                await serve()  # No settings — uses defaults

            mock_validator_cls.assert_called_once()
            call_kwargs = mock_validator_cls.call_args[1]
            assert call_kwargs["auth_url"] == "https://kbase.us/services/auth/"

    @pytest.mark.asyncio
    async def test_serve_registers_health_check(self) -> None:
        """serve() registers gRPC health check service."""
        mock_server = AsyncMock()
        mock_server.wait_for_termination = AsyncMock(side_effect=asyncio.CancelledError)
        mock_server.add_generic_rpc_handlers = MagicMock()
        mock_server.add_insecure_port = MagicMock()

        with (
            patch("spark_connect_proxy.proxy.aio.server", return_value=mock_server),
            patch("spark_connect_proxy.proxy.TokenValidator"),
            patch("spark_connect_proxy.proxy.ChannelPool") as mock_pool_cls,
            patch("spark_connect_proxy.proxy.health.HealthServicer") as mock_health_cls,
            patch(
                "spark_connect_proxy.proxy.health_pb2_grpc.add_HealthServicer_to_server"
            ) as mock_add,
        ):
            mock_pool = MagicMock()
            mock_pool.close_all = AsyncMock()
            mock_pool_cls.return_value = mock_pool

            mock_health = AsyncMock()
            mock_health_cls.return_value = mock_health

            with pytest.raises(asyncio.CancelledError):
                await serve(ProxySettings())

            # Health servicer should be added to the server
            mock_add.assert_called_once_with(mock_health, mock_server)
            # Health status should be set to SERVING
            assert mock_health.set.await_count >= 2  # overall + service-specific

    @pytest.mark.asyncio
    async def test_serve_listen_address(self) -> None:
        """serve() binds to the configured port."""
        mock_server = AsyncMock()
        mock_server.wait_for_termination = AsyncMock(side_effect=asyncio.CancelledError)
        mock_server.add_generic_rpc_handlers = MagicMock()
        mock_server.add_insecure_port = MagicMock()

        with (
            patch("spark_connect_proxy.proxy.aio.server", return_value=mock_server),
            patch("spark_connect_proxy.proxy.TokenValidator"),
            patch("spark_connect_proxy.proxy.ChannelPool") as mock_pool_cls,
            patch("spark_connect_proxy.proxy.health.HealthServicer") as mock_health_cls,
            patch("spark_connect_proxy.proxy.health_pb2_grpc.add_HealthServicer_to_server"),
        ):
            mock_pool = MagicMock()
            mock_pool.close_all = AsyncMock()
            mock_pool_cls.return_value = mock_pool

            mock_health = AsyncMock()
            mock_health_cls.return_value = mock_health

            settings = ProxySettings(PROXY_LISTEN_PORT=9999)

            with pytest.raises(asyncio.CancelledError):
                await serve(settings)

            mock_server.add_insecure_port.assert_called_once_with("[::]:9999")
