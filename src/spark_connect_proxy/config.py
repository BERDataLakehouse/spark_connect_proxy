"""Configuration for the Spark Connect gRPC proxy."""

from pydantic_settings import BaseSettings


class ProxySettings(BaseSettings):
    """Proxy server configuration, loaded from environment variables."""

    # KBase Auth2 service URL
    KBASE_AUTH_URL: str = "https://kbase.us/services/auth/"

    # Port the proxy server listens on
    PROXY_LISTEN_PORT: int = 15002

    # Port of Spark Connect on notebook pods
    BACKEND_PORT: int = 15002

    # Kubernetes namespace where notebook pods live
    BACKEND_NAMESPACE: str = "jupyterhub-prod"

    # Template for building backend service addresses.
    # {username} is replaced with the authenticated username.
    # {namespace} is replaced with BACKEND_NAMESPACE.
    SERVICE_TEMPLATE: str = "jupyter-{username}.{namespace}.svc.cluster.local"

    # How long (seconds) to cache validated tokens
    TOKEN_CACHE_TTL: int = 300

    # Maximum number of cached tokens
    TOKEN_CACHE_MAX_SIZE: int = 1000

    # Whether to require MFA for token validation
    REQUIRE_MFA: bool = True

    # Maximum concurrent gRPC connections to keep per backend
    MAX_CHANNELS_PER_BACKEND: int = 5

    model_config = {"env_prefix": ""}

    def backend_target(self, username: str) -> str:
        """Build the backend gRPC target for a given username."""
        host = self.SERVICE_TEMPLATE.format(
            username=username,
            namespace=self.BACKEND_NAMESPACE,
        )
        return f"{host}:{self.BACKEND_PORT}"
