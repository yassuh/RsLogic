"""Runtime entrypoint for the uvicorn API process."""

from __future__ import annotations

import logging

import uvicorn

from config import load_config


def main(host: str = "0.0.0.0", port: int = 8000) -> None:
    config = load_config()
    logging.basicConfig(level=getattr(logging, config.log.level, logging.INFO), format=config.log.format)
    logging.getLogger(__name__).info("Starting API server", extra={"host": host, "port": port, "log_level": config.log.level})
    uvicorn.run(
        "rslogic.api.app:app",
        host=host,
        port=port,
        reload=False,
        log_level=config.log.level.lower(),
        access_log=True,
    )


if __name__ == "__main__":
    main()
