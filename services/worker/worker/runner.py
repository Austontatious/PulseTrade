import os
import threading

from .app import app

try:
    from .sentiment import run_sentiment_loop
except ImportError:  # pragma: no cover
    run_sentiment_loop = None


def _start_sentiment_thread() -> None:
    if run_sentiment_loop is None:
        return
    import asyncio

    def _worker() -> None:
        asyncio.run(run_sentiment_loop())

    thread = threading.Thread(target=_worker, daemon=True)
    thread.start()


def main() -> None:
    if os.getenv("ENABLE_SENTIMENT_SCORING", "1") == "1":
        _start_sentiment_thread()

    queues = os.getenv("CELERY_QUEUES", "default,features,models")
    loglevel = os.getenv("CELERY_LOG_LEVEL", "INFO")
    argv = [
        "worker",
        "-l",
        loglevel,
        "-Q",
        queues,
    ]
    app.worker_main(argv)


if __name__ == "__main__":
    main()
