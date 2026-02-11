# spark-connect-proxy

[![Tests](https://github.com/BERDataLakehouse/spark_connect_proxy/actions/workflows/test.yml/badge.svg)](https://github.com/BERDataLakehouse/spark_connect_proxy/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/BERDataLakehouse/spark_connect_proxy/graph/badge.svg?token=CODECOV_TOKEN)](https://codecov.io/gh/BERDataLakehouse/spark_connect_proxy)
[![Python 3.13](https://img.shields.io/badge/python-3.13-blue.svg)](https://www.python.org/downloads/release/python-3130/)

gRPC proxy for multi-user Spark Connect access via KBase authentication.

Routes PySpark Spark Connect gRPC traffic from a single endpoint to the correct user's notebook pod based on the KBase token in the request metadata.

## Architecture

```
User (PySpark) → Ingress (spark.berdl.kbase.us:443) → Spark Connect Proxy → jupyter-{username}:15002
```

1. User sends gRPC request with `x-kbase-token` metadata
2. Proxy validates token, resolves username
3. Proxy forwards gRPC to `jupyter-{username}.jupyterhub-prod.svc.cluster.local:15002`
4. Responses stream back to user transparently

## Configuration

| Env Var | Description | Default |
|---------|-------------|---------|
| `KBASE_AUTH_URL` | KBase Auth2 service URL | `https://kbase.us/services/auth/` |
| `PROXY_LISTEN_PORT` | Port the proxy listens on | `15002` |
| `BACKEND_PORT` | Spark Connect port on notebooks | `15002` |
| `BACKEND_NAMESPACE` | K8s namespace for notebooks | `jupyterhub-prod` |
| `SERVICE_TEMPLATE` | Backend service pattern | `jupyter-{username}.{namespace}.svc.cluster.local` |
| `TOKEN_CACHE_TTL` | Token cache TTL (seconds) | `300` |

## Development

```bash
# Install
pip install -e ".[dev]"

# Run tests
pytest

# Run locally
python -m spark_connect_proxy
```
