from __future__ import annotations

from django.core.management.base import BaseCommand

from rq import Connection, Queue, Worker

from core.queues import redis_conn


class Command(BaseCommand):
    help = "Run an RQ worker with the configured Redis connection."

    def add_arguments(self, parser):
        parser.add_argument(
            "queues",
            nargs="*",
            default=["default"],
            help="Queue names to listen to (defaults to 'default').",
        )
        parser.add_argument(
            "--burst",
            action="store_true",
            help="Run in burst mode and exit when the queues are empty.",
        )

    def handle(self, *args, **options):
        queue_names = options["queues"] or ["default"]
        queues = [Queue(name, connection=redis_conn) for name in queue_names]
        burst = options["burst"]

        self.stdout.write(
            self.style.NOTICE(
                f"Starting RQ worker for queues: {', '.join(queue_names)}{' (burst)' if burst else ''}",
            ),
        )

        with Connection(redis_conn):
            worker = Worker(queues)
            worker.work(burst=burst)
