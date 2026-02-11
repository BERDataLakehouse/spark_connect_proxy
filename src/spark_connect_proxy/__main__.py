"""Entry point for the Spark Connect gRPC proxy."""

import asyncio
import logging

from spark_connect_proxy.proxy import serve


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    asyncio.run(serve())


if __name__ == "__main__":
    main()
