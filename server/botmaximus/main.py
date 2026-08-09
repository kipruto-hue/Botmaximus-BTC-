"""Entrypoint: `python -m botmaximus.main` — starts the pipeline + API in one process."""
import logging

import uvicorn

from botmaximus.config import settings
from botmaximus.storage import postgres

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
)


def run() -> None:
    # Must happen before uvicorn creates its loop: on Windows the default
    # ProactorEventLoop is one psycopg refuses to run on, and every database
    # call would fail looking like an outage rather than a platform default.
    postgres.ensure_compatible_event_loop()
    uvicorn.run(
        "botmaximus.api.app:app",
        host=settings.api_host,
        port=settings.api_port,
        log_level="info",
    )


if __name__ == "__main__":
    run()
