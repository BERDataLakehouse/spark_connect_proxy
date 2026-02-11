"""Tests for the proxy configuration."""

import os
from unittest.mock import patch

from spark_connect_proxy.config import ProxySettings


class TestProxySettings:
    """Tests for ProxySettings."""

    def test_defaults(self) -> None:
        """Default settings are sensible."""
        settings = ProxySettings()
        assert settings.KBASE_AUTH_URL == "https://kbase.us/services/auth/"
        assert settings.PROXY_LISTEN_PORT == 15002
        assert settings.BACKEND_PORT == 15002
        assert settings.BACKEND_NAMESPACE == "jupyterhub-prod"
        assert "{username}" in settings.SERVICE_TEMPLATE

    def test_backend_target(self) -> None:
        """backend_target() builds the correct address."""
        settings = ProxySettings()
        target = settings.backend_target("alice")
        assert target == "jupyter-alice.jupyterhub-prod.svc.cluster.local:15002"

    def test_backend_target_custom_template(self) -> None:
        """Custom SERVICE_TEMPLATE is respected."""
        settings = ProxySettings(
            SERVICE_TEMPLATE="spark-{username}.{namespace}.internal",
            BACKEND_NAMESPACE="dev",
            BACKEND_PORT=50051,
        )
        target = settings.backend_target("bob")
        assert target == "spark-bob.dev.internal:50051"

    @patch.dict(
        os.environ,
        {
            "KBASE_AUTH_URL": "https://custom.auth.url/",
            "PROXY_LISTEN_PORT": "9999",
            "BACKEND_NAMESPACE": "testing",
        },
    )
    def test_from_env_vars(self) -> None:
        """Settings can be loaded from environment variables."""
        settings = ProxySettings()
        assert settings.KBASE_AUTH_URL == "https://custom.auth.url/"
        assert settings.PROXY_LISTEN_PORT == 9999
        assert settings.BACKEND_NAMESPACE == "testing"
