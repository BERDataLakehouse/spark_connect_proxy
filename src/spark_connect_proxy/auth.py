"""KBase token validation with TTL caching and MFA enforcement."""

import logging
import os
import time
from enum import IntEnum

import httpx

logger = logging.getLogger(__name__)


class AuthError(Exception):
    """Raised when token validation fails."""


class MissingMFAError(AuthError):
    """Raised when a token was not created with MFA and MFA is required."""


class MFAStatus(IntEnum):
    """MFA status for a token."""

    NOT_USED = 0  # Token was not created with MFA
    USED = 1  # Token was created with MFA
    UNKNOWN = 2  # MFA status could not be determined


class TokenValidator:
    """
    Validates KBase authentication tokens with TTL-based caching.

    Caches (token → username) mappings so that repeated gRPC calls from the
    same token don't hit the Auth2 service on every request.

    Optionally enforces MFA — tokens not created with MFA will be rejected
    unless the user is in the MFA_EXEMPT_USERS environment variable list.
    """

    def __init__(
        self,
        auth_url: str = "https://kbase.us/services/auth/",
        cache_ttl: int = 300,
        cache_max_size: int = 1000,
        timeout: float = 10.0,
        require_mfa: bool = True,
    ):
        self._auth_url = auth_url.rstrip("/") + "/"
        self._cache_ttl = cache_ttl
        self._cache_max_size = cache_max_size
        self._timeout = timeout
        self._require_mfa = require_mfa
        # Read MFA-exempt users from environment variable (comma-separated list)
        mfa_exempt_env = os.getenv("MFA_EXEMPT_USERS", "")
        self._mfa_exempt_users = {u.strip() for u in mfa_exempt_env.split(",") if u.strip()}
        # Cache: token → (username, mfa_status, expiry_timestamp)
        self._cache: dict[str, tuple[str, MFAStatus, float]] = {}

    def get_username(self, token: str) -> str:
        """
        Validate a token and return the associated username.

        Uses a TTL cache to avoid hitting Auth2 for every gRPC call.
        Optionally enforces MFA based on the require_mfa flag.

        Args:
            token: KBase authentication token.

        Returns:
            The username associated with the token.

        Raises:
            AuthError: If the token is missing, invalid, or expired.
            MissingMFAError: If MFA is required and the token was not created with MFA.
        """
        if not token or not token.strip():
            raise AuthError("Missing authentication token")

        token = token.strip()

        # Check cache
        cached = self._cache.get(token)
        if cached is not None:
            username, mfa_status, expiry = cached
            if time.monotonic() < expiry:
                self._check_mfa(username, mfa_status)
                return username
            # Expired — remove from cache
            del self._cache[token]

        # Validate against KBase Auth2
        username, mfa_status = self._validate_remote(token)

        # Check MFA requirement
        self._check_mfa(username, mfa_status)

        # Cache the result
        self._evict_if_full()
        self._cache[token] = (username, mfa_status, time.monotonic() + self._cache_ttl)

        return username

    def _check_mfa(self, username: str, mfa_status: MFAStatus) -> None:
        """Check MFA requirement, skip for exempt users (e.g. service accounts)."""
        if not self._require_mfa:
            return
        is_exempt = username in self._mfa_exempt_users
        if mfa_status != MFAStatus.USED and not is_exempt:
            raise MissingMFAError(
                "This service requires multi-factor authentication (MFA). "
                "Please log in with MFA enabled to access this service."
            )

    def _validate_remote(self, token: str) -> tuple[str, MFAStatus]:
        """Call KBase Auth2 to validate the token and check MFA status."""
        url = f"{self._auth_url}api/V2/token"

        try:
            response = httpx.get(
                url,
                headers={"Authorization": token},
                timeout=self._timeout,
            )
        except httpx.TimeoutException as e:
            raise AuthError("Auth service timed out") from e
        except httpx.HTTPError as e:
            raise AuthError(f"Auth service error: {e}") from e

        if response.status_code == 401:
            raise AuthError("Invalid or expired token")

        if response.status_code != 200:
            raise AuthError(f"Auth service returned status {response.status_code}")

        try:
            data = response.json()
            username = data["user"]
        except (ValueError, KeyError) as e:
            raise AuthError(f"Invalid auth response: {e}") from e

        # Parse MFA status from response
        mfa_value = data.get("mfa", "")
        if mfa_value == "Used":
            mfa_status = MFAStatus.USED
        elif mfa_value in ("", "NotUsed", None):
            mfa_status = MFAStatus.NOT_USED
        else:
            mfa_status = MFAStatus.UNKNOWN

        logger.info("Authenticated user: %s (MFA: %s)", username, mfa_status.name)
        return username, mfa_status

    def _evict_if_full(self) -> None:
        """Evict oldest entries if the cache exceeds max size."""
        if len(self._cache) >= self._cache_max_size:
            # Remove the oldest 10% of entries
            entries = sorted(self._cache.items(), key=lambda x: x[1][2])
            to_remove = max(1, len(entries) // 10)
            for key, _ in entries[:to_remove]:
                del self._cache[key]

    def invalidate(self, token: str) -> None:
        """Remove a token from the cache."""
        self._cache.pop(token, None)

    def clear_cache(self) -> None:
        """Clear the entire token cache."""
        self._cache.clear()
