"""KBase token validation with TTL caching."""

import logging
import time

import httpx

logger = logging.getLogger(__name__)


class AuthError(Exception):
    """Raised when token validation fails."""


class TokenValidator:
    """
    Validates KBase authentication tokens with TTL-based caching.

    Caches (token → username) mappings so that repeated gRPC calls from the
    same token don't hit the Auth2 service on every request.
    """

    def __init__(
        self,
        auth_url: str = "https://kbase.us/services/auth/",
        cache_ttl: int = 300,
        cache_max_size: int = 1000,
        timeout: float = 10.0,
    ):
        self._auth_url = auth_url.rstrip("/") + "/"
        self._cache_ttl = cache_ttl
        self._cache_max_size = cache_max_size
        self._timeout = timeout
        # Cache: token → (username, expiry_timestamp)
        self._cache: dict[str, tuple[str, float]] = {}

    def get_username(self, token: str) -> str:
        """
        Validate a token and return the associated username.

        Uses a TTL cache to avoid hitting Auth2 for every gRPC call.

        Args:
            token: KBase authentication token.

        Returns:
            The username associated with the token.

        Raises:
            AuthError: If the token is missing, invalid, or expired.
        """
        if not token or not token.strip():
            raise AuthError("Missing authentication token")

        token = token.strip()

        # Check cache
        cached = self._cache.get(token)
        if cached is not None:
            username, expiry = cached
            if time.monotonic() < expiry:
                return username
            # Expired — remove from cache
            del self._cache[token]

        # Validate against KBase Auth2
        username = self._validate_remote(token)

        # Cache the result
        self._evict_if_full()
        self._cache[token] = (username, time.monotonic() + self._cache_ttl)

        return username

    def _validate_remote(self, token: str) -> str:
        """Call KBase Auth2 to validate the token."""
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

        logger.info("Authenticated user: %s", username)
        return username

    def _evict_if_full(self) -> None:
        """Evict oldest entries if the cache exceeds max size."""
        if len(self._cache) >= self._cache_max_size:
            # Remove the oldest 10% of entries
            entries = sorted(self._cache.items(), key=lambda x: x[1][1])
            to_remove = max(1, len(entries) // 10)
            for key, _ in entries[:to_remove]:
                del self._cache[key]

    def invalidate(self, token: str) -> None:
        """Remove a token from the cache."""
        self._cache.pop(token, None)

    def clear_cache(self) -> None:
        """Clear the entire token cache."""
        self._cache.clear()
