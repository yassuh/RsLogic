"""Standalone Redis queue worker for processing and upload jobs."""

from __future__ import annotations

import logging
import signal
import threading
import time

from config import load_config
from rslogic.jobs.service import ImageUploadOrchestrator, JobOrchestrator
from rslogic.storage import StorageRepository

logger = logging.getLogger("rslogic.jobs.worker")


def main() -> None:
    config = load_config()
    logging.basicConfig(
        level=getattr(logging, config.log.level, logging.INFO),
        format=config.log.format,
    )

    if config.queue.backend != "redis":
        raise SystemExit("rslogic-worker requires RSLOGIC_QUEUE_BACKEND=redis")

    repository = StorageRepository()
    processing_orchestrator = JobOrchestrator(
        repository=repository,
        max_workers=config.queue.worker_count,
        config=config,
        start_workers=True,
    )
    upload_orchestrator = ImageUploadOrchestrator(
        repository=repository,
        max_workers=config.queue.worker_count,
        config=config,
        start_workers=True,
    )

    stop_event = threading.Event()

    def _request_stop(signum, _frame) -> None:
        logger.info("Worker shutdown requested signal=%s", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    logger.info(
        "Redis worker running queue_base=%s workers_per_queue=%s redis_url=%s",
        config.queue.redis_queue_key,
        config.queue.worker_count,
        config.queue.redis_url,
    )
    try:
        while not stop_event.is_set():
            time.sleep(max(config.queue.poll_interval_seconds, 1))
    finally:
        processing_orchestrator.close()
        upload_orchestrator.close()
        logger.info("Redis worker stopped")


if __name__ == "__main__":
    main()
