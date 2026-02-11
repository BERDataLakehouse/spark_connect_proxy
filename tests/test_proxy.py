"""Tests for the gRPC proxy handler."""

from unittest.mock import MagicMock, patch

import grpc
import pytest

from spark_connect_proxy.auth import AuthError, TokenValidator
from spark_connect_proxy.config import ProxySettings
from spark_connect_proxy.proxy import (
    ChannelPool,
    SparkConnectProxyHandler,
    _extract_token,
)


class TestExtractToken:
    """Tests for _extract_token()."""

    def test_extracts_token(self) -> None:
        metadata = (("x-kbase-token", "my-token"), ("other", "value"))
        assert _extract_token(metadata) == "my-token"

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


class TestSparkConnectProxyHandler:
    """Tests for SparkConnectProxyHandler routing logic."""

    def setup_method(self) -> None:
        self.settings = ProxySettings(
            BACKEND_NAMESPACE="test-ns",
            SERVICE_TEMPLATE="jupyter-{username}.{namespace}.svc.local",
        )
        self.validator = MagicMock(spec=TokenValidator)
        self.pool = MagicMock(spec=ChannelPool)
        self.handler = SparkConnectProxyHandler(
            self.settings, self.validator, self.pool
        )

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
        self.pool.get_channel.assert_called_once_with(
            "jupyter-alice.test-ns.svc.local:15002"
        )

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
        details = self._make_call_details(
            "/spark.connect.SparkConnectService/Config", metadata
        )
        result = self.handler.service(details)
        # Should return an error handler, not None
        assert result is not None
        self.pool.get_channel.assert_not_called()

    def test_invalid_token_returns_unauthenticated_handler(self) -> None:
        """Invalid token returns an error handler."""
        self.validator.get_username.side_effect = AuthError("Invalid token")
        metadata = (("x-kbase-token", "bad"),)
        details = self._make_call_details(
            "/spark.connect.SparkConnectService/Config", metadata
        )
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
        self.pool.get_channel.assert_called_with(
            "jupyter-bsadkhin.test-ns.svc.local:15002"
        )
