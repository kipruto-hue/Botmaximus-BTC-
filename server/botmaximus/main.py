"""Entrypoint: `python -m botmaximus.main` — starts the pipeline + API in one process."""
import logging

import uvicorn

from botmaximus.config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
)


def run() -> None:
    uvicorn.run(
        "botmaximus.api.app:app",
        host=settings.api_host,
        port=settings.api_port,
        log_level="info",
    )


if __name__ == "__main__":
    run()
