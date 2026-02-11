"""Tests for the KBase token validator with TTL caching and MFA enforcement."""

import time
from unittest.mock import MagicMock, patch

import pytest

from spark_connect_proxy.auth import AuthError, MFAStatus, MissingMFAError, TokenValidator


class TestTokenValidator:
    """Tests for TokenValidator."""

    def setup_method(self) -> None:
        self.validator = TokenValidator(
            auth_url="https://auth.example.com/",
            cache_ttl=60,
            cache_max_size=10,
            require_mfa=False,  # Disable MFA for basic tests
        )

    @patch("spark_connect_proxy.auth.httpx.get")
    def test_valid_token(self, mock_get: MagicMock) -> None:
        """Valid token returns the username."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"user": "alice", "mfa": "Used"}
        mock_get.return_value = mock_response

        username = self.validator.get_username("valid-token")
        assert username == "alice"

        mock_get.assert_called_once_with(
            "https://auth.example.com/api/V2/token",
            headers={"Authorization": "valid-token"},
            timeout=10.0,
        )

    @patch("spark_connect_proxy.auth.httpx.get")
    def test_invalid_token_401(self, mock_get: MagicMock) -> None:
        """401 response raises AuthError."""
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_get.return_value = mock_response

        with pytest.raises(AuthError, match="Invalid or expired token"):
            self.validator.get_username("bad-token")

    @patch("spark_connect_proxy.auth.httpx.get")
    def test_server_error(self, mock_get: MagicMock) -> None:
        """Non-200/401 status raises AuthError."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        with pytest.raises(AuthError, match="status 500"):
            self.validator.get_username("some-token")

    def test_empty_token(self) -> None:
        """Empty or whitespace token raises AuthError."""
        with pytest.raises(AuthError, match="Missing"):
            self.validator.get_username("")

        with pytest.raises(AuthError, match="Missing"):
            self.validator.get_username("   ")

    @patch("spark_connect_proxy.auth.httpx.get")
    def test_cache_hit(self, mock_get: MagicMock) -> None:
        """Second call with same token uses cache, doesn't call Auth2."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"user": "bob", "mfa": "Used"}
        mock_get.return_value = mock_response

        # First call — hits Auth2
        assert self.validator.get_username("token-1") == "bob"
        assert mock_get.call_count == 1

        # Second call — cached
        assert self.validator.get_username("token-1") == "bob"
        assert mock_get.call_count == 1  # no additional call

    @patch("spark_connect_proxy.auth.httpx.get")
    def test_cache_expiry(self, mock_get: MagicMock) -> None:
        """Expired cache entries trigger a new Auth2 call."""
        validator = TokenValidator(
            auth_url="https://auth.example.com/",
            cache_ttl=1,  # 1 second TTL
            require_mfa=False,
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"user": "carol", "mfa": "Used"}
        mock_get.return_value = mock_response

        # First call
        assert validator.get_username("token-2") == "carol"
        assert mock_get.call_count == 1

        # Wait for cache to expire
        time.sleep(1.1)

        # Should call Auth2 again
        assert validator.get_username("token-2") == "carol"
        assert mock_get.call_count == 2

    @patch("spark_connect_proxy.auth.httpx.get")
    def test_cache_eviction(self, mock_get: MagicMock) -> None:
        """Cache evicts oldest entries when full."""
        validator = TokenValidator(
            auth_url="https://auth.example.com/",
            cache_ttl=3600,
            cache_max_size=5,
            require_mfa=False,
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"user": "user", "mfa": "Used"}
        mock_get.return_value = mock_response

        # Fill the cache
        for i in range(6):
            validator.get_username(f"token-{i}")

        # Cache should not exceed max size
        assert len(validator._cache) <= 5

    @patch("spark_connect_proxy.auth.httpx.get")
    def test_invalidate(self, mock_get: MagicMock) -> None:
        """invalidate() removes a token from cache."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"user": "dave", "mfa": "Used"}
        mock_get.return_value = mock_response

        self.validator.get_username("token-x")
        assert "token-x" in self.validator._cache

        self.validator.invalidate("token-x")
        assert "token-x" not in self.validator._cache

    @patch("spark_connect_proxy.auth.httpx.get")
    def test_clear_cache(self, mock_get: MagicMock) -> None:
        """clear_cache() empties the entire cache."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"user": "eve", "mfa": "Used"}
        mock_get.return_value = mock_response

        self.validator.get_username("t1")
        self.validator.get_username("t2")
        assert len(self.validator._cache) == 2

        self.validator.clear_cache()
        assert len(self.validator._cache) == 0


class TestMFAEnforcement:
    """Tests for MFA enforcement in TokenValidator."""

    @patch("spark_connect_proxy.auth.httpx.get")
    def test_mfa_used_accepted(self, mock_get: MagicMock) -> None:
        """Token with MFA Used is accepted when require_mfa is True."""
        validator = TokenValidator(
            auth_url="https://auth.example.com/",
            require_mfa=True,
        )
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"user": "alice", "mfa": "Used"}
        mock_get.return_value = mock_response

        assert validator.get_username("mfa-token") == "alice"

    @patch("spark_connect_proxy.auth.httpx.get")
    def test_mfa_not_used_rejected(self, mock_get: MagicMock) -> None:
        """Token without MFA is rejected when require_mfa is True."""
        validator = TokenValidator(
            auth_url="https://auth.example.com/",
            require_mfa=True,
        )
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"user": "alice", "mfa": "NotUsed"}
        mock_get.return_value = mock_response

        with pytest.raises(MissingMFAError, match="multi-factor authentication"):
            validator.get_username("non-mfa-token")

    @patch("spark_connect_proxy.auth.httpx.get")
    def test_mfa_missing_field_rejected(self, mock_get: MagicMock) -> None:
        """Token with no mfa field is rejected when require_mfa is True."""
        validator = TokenValidator(
            auth_url="https://auth.example.com/",
            require_mfa=True,
        )
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"user": "alice"}
        mock_get.return_value = mock_response

        with pytest.raises(MissingMFAError, match="multi-factor authentication"):
            validator.get_username("no-mfa-field-token")

    @patch("spark_connect_proxy.auth.httpx.get")
    def test_mfa_not_required(self, mock_get: MagicMock) -> None:
        """Token without MFA is accepted when require_mfa is False."""
        validator = TokenValidator(
            auth_url="https://auth.example.com/",
            require_mfa=False,
        )
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"user": "alice", "mfa": "NotUsed"}
        mock_get.return_value = mock_response

        assert validator.get_username("non-mfa-token") == "alice"

    @patch.dict("os.environ", {"MFA_EXEMPT_USERS": "serviceaccount,botuser"})
    @patch("spark_connect_proxy.auth.httpx.get")
    def test_mfa_exempt_user(self, mock_get: MagicMock) -> None:
        """Exempt users bypass MFA check even when require_mfa is True."""
        validator = TokenValidator(
            auth_url="https://auth.example.com/",
            require_mfa=True,
        )
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"user": "serviceaccount", "mfa": "NotUsed"}
        mock_get.return_value = mock_response

        # Should not raise even though MFA is not used
        assert validator.get_username("service-token") == "serviceaccount"

    @patch("spark_connect_proxy.auth.httpx.get")
    def test_mfa_status_parsed_correctly(self, mock_get: MagicMock) -> None:
        """MFA status values are parsed correctly."""
        validator = TokenValidator(
            auth_url="https://auth.example.com/",
            require_mfa=False,
        )

        # Test "Used"
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"user": "a", "mfa": "Used"}
        mock_get.return_value = mock_response

        validator.get_username("t1")
        assert validator._cache["t1"][1] == MFAStatus.USED

        # Test "NotUsed"
        mock_response.json.return_value = {"user": "b", "mfa": "NotUsed"}
        validator.get_username("t2")
        assert validator._cache["t2"][1] == MFAStatus.NOT_USED

        # Test empty string
        mock_response.json.return_value = {"user": "c", "mfa": ""}
        validator.get_username("t3")
        assert validator._cache["t3"][1] == MFAStatus.NOT_USED

        # Test unknown value
        mock_response.json.return_value = {"user": "d", "mfa": "SomethingElse"}
        validator.get_username("t4")
        assert validator._cache["t4"][1] == MFAStatus.UNKNOWN

    @patch("spark_connect_proxy.auth.httpx.get")
    def test_mfa_checked_on_cache_hit(self, mock_get: MagicMock) -> None:
        """MFA is re-checked on cache hits (in case policy changed)."""
        validator = TokenValidator(
            auth_url="https://auth.example.com/",
            require_mfa=False,
        )
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"user": "alice", "mfa": "NotUsed"}
        mock_get.return_value = mock_response

        # First call succeeds (MFA not required)
        assert validator.get_username("token") == "alice"

        # Enable MFA requirement
        validator._require_mfa = True

        # Second call should fail from cache (MFA check on cache hit)
        with pytest.raises(MissingMFAError):
            validator.get_username("token")
