"""Entrypoint: `python -m botmaximus.main` — starts the pipeline + API in one process."""
import asyncio
import logging
import sys

import uvicorn

from botmaximus.config import settings
from botmaximus.storage import postgres

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
)


def run() -> None:
    # Two separate things have to be right on Windows, and only the first was.
    #
    # `ensure_compatible_event_loop()` sets the event-loop *policy*, which is
    # what every other entrypoint (tests, the ops scripts) goes through. But
    # `uvicorn.run()` does not consult the policy: it calls asyncio.run with an
    # explicit `loop_factory`, which on Windows is `ProactorEventLoop` — the one
    # loop psycopg refuses to run on. So the server always came up on the wrong
    # loop and every database call failed as "pool initialization incomplete
    # after 10 sec", i.e. as an outage rather than as a platform default.
    #
    # Owning the loop here is the fix: uvicorn is handed a running, compatible
    # loop instead of being asked to make one. Production is Linux, where the
    # branch below is not taken and behaviour is unchanged.
    postgres.ensure_compatible_event_loop()
    server = uvicorn.Server(uvicorn.Config(
        "botmaximus.api.app:app",
        host=settings.api_host,
        port=settings.api_port,
        log_level="info",
    ))
    if sys.platform == "win32":
        asyncio.run(server.serve(), loop_factory=asyncio.SelectorEventLoop)
    else:
        server.run()


if __name__ == "__main__":
    run()
