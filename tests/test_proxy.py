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
    """Tests for ChannelPool."""

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

        # Create an async iterator for the streaming response
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

    def test_valid_unary_unary(self) -> None:
        """Valid token for a unary-unary method returns a handler."""
        self.validator.get_username.return_value = "alice"
        metadata = (("x-kbase-token", "valid"),)
        details = self._make_call_details(
            "/spark.connect.SparkConnectService/AnalyzePlan", metadata
        )
        result = self.handler.service(details)
        assert result is not None
        self.validator.get_username.assert_called_once_with("valid")
        self.pool.get_channel.assert_called_once_with("jupyter-alice.test-ns.svc.local:15002")

    def test_valid_unary_stream(self) -> None:
        """Valid token for a server-streaming method returns a handler."""
        self.validator.get_username.return_value = "bob"
        metadata = (("x-kbase-token", "valid"),)
        details = self._make_call_details(
            "/spark.connect.SparkConnectService/ExecutePlan", metadata
        )
        result = self.handler.service(details)
        assert result is not None

    def test_valid_stream_unary(self) -> None:
        """Valid token for a client-streaming method returns a handler."""
        self.validator.get_username.return_value = "carol"
        metadata = (("x-kbase-token", "valid"),)
        details = self._make_call_details(
            "/spark.connect.SparkConnectService/AddArtifacts", metadata
        )
        result = self.handler.service(details)
        assert result is not None

    def test_missing_token_returns_unauthenticated_handler(self) -> None:
        """Missing token returns an error handler (not None)."""
        metadata = (("other-header", "value"),)
        details = self._make_call_details("/spark.connect.SparkConnectService/Config", metadata)
        result = self.handler.service(details)
        # Should return an error handler, not None
        assert result is not None
        self.pool.get_channel.assert_not_called()

    def test_invalid_token_returns_unauthenticated_handler(self) -> None:
        """Invalid token returns an error handler."""
        self.validator.get_username.side_effect = AuthError("Invalid token")
        metadata = (("x-kbase-token", "bad"),)
        details = self._make_call_details("/spark.connect.SparkConnectService/Config", metadata)
        result = self.handler.service(details)
        assert result is not None
        self.pool.get_channel.assert_not_called()

    def test_routes_to_correct_user(self) -> None:
        """Different tokens route to different backends."""
        # First user
        self.validator.get_username.return_value = "tgu2"
        details = self._make_call_details(
            "/spark.connect.SparkConnectService/Config",
            (("x-kbase-token", "token-tgu2"),),
        )
        self.handler.service(details)
        self.pool.get_channel.assert_called_with("jupyter-tgu2.test-ns.svc.local:15002")

        # Second user
        self.pool.reset_mock()
        self.validator.get_username.return_value = "bsadkhin"
        details = self._make_call_details(
            "/spark.connect.SparkConnectService/Config",
            (("x-kbase-token", "token-boris"),),
        )
        self.handler.service(details)
        self.pool.get_channel.assert_called_with("jupyter-bsadkhin.test-ns.svc.local:15002")


# ---------------------------------------------------------------------------
# Handler behavior invocation tests
# ---------------------------------------------------------------------------


class TestHandlerBehaviors:
    """Tests that actually invoke the async handler behaviors returned by service()."""

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
    async def test_unary_unary_behavior_invokes_proxy(self) -> None:
        """The unary-unary handler behavior calls _proxy_unary_unary."""
        metadata = (("x-kbase-token", "tok"),)
        details = self._make_call_details("/spark.connect.SparkConnectService/Config", metadata)
        result = self.handler.service(details)
        assert result is not None

        # Patch the channel's unary_unary to return a callable
        with patch("spark_connect_proxy.proxy._proxy_unary_unary", new_callable=AsyncMock) as mock:
            mock.return_value = b"response"
            context = MagicMock()
            response = await result.unary_unary(b"request", context)
            assert response == b"response"

    @pytest.mark.asyncio
    async def test_unary_stream_behavior_invokes_proxy(self) -> None:
        """The unary-stream handler behavior calls _proxy_unary_stream."""
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
            context = MagicMock()
            chunks = []
            async for chunk in result.unary_stream(b"request", context):
                chunks.append(chunk)
            assert chunks == [b"chunk-1", b"chunk-2"]

    @pytest.mark.asyncio
    async def test_stream_unary_behavior_invokes_proxy(self) -> None:
        """The stream-unary handler behavior calls _proxy_stream_unary."""
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

            context = MagicMock()
            response = await result.stream_unary(request_iter(), context)
            assert response == b"aggregated"


# ---------------------------------------------------------------------------
# Unauthenticated handler tests
# ---------------------------------------------------------------------------


class TestUnauthenticatedHandlers:
    """Tests for _unauthenticated_handler for all RPC types."""

    def setup_method(self) -> None:
        self.settings = ProxySettings(
            BACKEND_NAMESPACE="test-ns",
            SERVICE_TEMPLATE="jupyter-{username}.{namespace}.svc.local",
        )
        self.validator = MagicMock(spec=TokenValidator)
        self.pool = MagicMock(spec=ChannelPool)
        self.handler = SparkConnectProxyHandler(self.settings, self.validator, self.pool)

    def test_unauthenticated_unary_unary(self) -> None:
        """Returns unary-unary abort handler for unary-unary methods."""
        result = self.handler._unauthenticated_handler("bad token", "unary", "unary")
        assert result is not None
        assert result.unary_unary is not None

    def test_unauthenticated_unary_stream(self) -> None:
        """Returns unary-stream abort handler for server-streaming methods."""
        result = self.handler._unauthenticated_handler("bad token", "unary", "stream")
        assert result is not None
        assert result.unary_stream is not None

    def test_unauthenticated_stream_unary(self) -> None:
        """Returns stream-unary abort handler for client-streaming methods."""
        result = self.handler._unauthenticated_handler("bad token", "stream", "unary")
        assert result is not None
        assert result.stream_unary is not None

    def test_unauthenticated_unknown_type_fallback(self) -> None:
        """Unknown RPC types fall back to unary-unary abort handler."""
        result = self.handler._unauthenticated_handler("bad token", "stream", "stream")
        assert result is not None
        assert result.unary_unary is not None

    @pytest.mark.asyncio
    async def test_abort_unary_calls_context_abort(self) -> None:
        """The unary abort handler calls context.abort with UNAUTHENTICATED."""
        result = self.handler._unauthenticated_handler("invalid", "unary", "unary")
        context = AsyncMock()
        context.abort = AsyncMock(side_effect=grpc.RpcError())

        with pytest.raises(grpc.RpcError):
            await result.unary_unary(b"req", context)
        context.abort.assert_awaited_once_with(grpc.StatusCode.UNAUTHENTICATED, "invalid")

    @pytest.mark.asyncio
    async def test_abort_stream_calls_context_abort(self) -> None:
        """The stream abort handler calls context.abort with UNAUTHENTICATED."""
        result = self.handler._unauthenticated_handler("invalid", "unary", "stream")
        context = AsyncMock()
        context.abort = AsyncMock(side_effect=grpc.RpcError())

        with pytest.raises(grpc.RpcError):
            async for _ in result.unary_stream(b"req", context):
                pass
        context.abort.assert_awaited_once_with(grpc.StatusCode.UNAUTHENTICATED, "invalid")

    @pytest.mark.asyncio
    async def test_abort_client_stream_calls_context_abort(self) -> None:
        """The client-stream abort handler calls context.abort with UNAUTHENTICATED."""
        result = self.handler._unauthenticated_handler("invalid", "stream", "unary")
        context = AsyncMock()
        context.abort = AsyncMock(side_effect=grpc.RpcError())

        async def request_iter():
            yield b"data"

        with pytest.raises(grpc.RpcError):
            await result.stream_unary(request_iter(), context)
        context.abort.assert_awaited_once_with(grpc.StatusCode.UNAUTHENTICATED, "invalid")


# ---------------------------------------------------------------------------
# serve() function tests
# ---------------------------------------------------------------------------


class TestServe:
    """Tests for the serve() entry point."""

    @pytest.mark.asyncio
    async def test_serve_starts_and_stops(self) -> None:
        """serve() creates a server, starts it, and can be shut down."""
        mock_server = AsyncMock()
        # Simulate termination by raising an exception
        mock_server.wait_for_termination = AsyncMock(side_effect=asyncio.CancelledError)

        with (
            patch("spark_connect_proxy.proxy.aio.server", return_value=mock_server),
            patch("spark_connect_proxy.proxy.TokenValidator"),
            patch("spark_connect_proxy.proxy.ChannelPool") as mock_pool_cls,
        ):
            mock_pool = MagicMock()
            mock_pool.close_all = AsyncMock()
            mock_pool_cls.return_value = mock_pool

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

        with (
            patch("spark_connect_proxy.proxy.aio.server", return_value=mock_server),
            patch("spark_connect_proxy.proxy.TokenValidator") as mock_validator_cls,
            patch("spark_connect_proxy.proxy.ChannelPool") as mock_pool_cls,
        ):
            mock_pool = MagicMock()
            mock_pool.close_all = AsyncMock()
            mock_pool_cls.return_value = mock_pool

            with pytest.raises(asyncio.CancelledError):
                await serve()  # No settings — uses defaults

            mock_validator_cls.assert_called_once()
            call_kwargs = mock_validator_cls.call_args[1]
            assert call_kwargs["auth_url"] == "https://kbase.us/services/auth/"

    @pytest.mark.asyncio
    async def test_serve_listen_address(self) -> None:
        """serve() binds to the configured port."""
        mock_server = AsyncMock()
        mock_server.wait_for_termination = AsyncMock(side_effect=asyncio.CancelledError)

        with (
            patch("spark_connect_proxy.proxy.aio.server", return_value=mock_server),
            patch("spark_connect_proxy.proxy.TokenValidator"),
            patch("spark_connect_proxy.proxy.ChannelPool") as mock_pool_cls,
        ):
            mock_pool = MagicMock()
            mock_pool.close_all = AsyncMock()
            mock_pool_cls.return_value = mock_pool

            settings = ProxySettings(PROXY_LISTEN_PORT=9999)

            with pytest.raises(asyncio.CancelledError):
                await serve(settings)

            mock_server.add_insecure_port.assert_called_once_with("[::]:9999")
