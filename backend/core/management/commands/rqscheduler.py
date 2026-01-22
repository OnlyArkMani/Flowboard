from __future__ import annotations

from django.core.management.base import BaseCommand

from rq_scheduler import Scheduler

from core.queues import redis_conn


class Command(BaseCommand):
    help = "Run the RQ scheduler loop so cron-based jobs are enqueued."

    def add_arguments(self, parser):
        parser.add_argument(
            "--queue",
            default="default",
            help="Queue name that scheduled jobs should be enqueued into.",
        )
        parser.add_argument(
            "--interval",
            type=int,
            default=60,
            help="Polling interval in seconds (default: 60).",
        )

    def handle(self, *args, **options):
        queue_name = options["queue"]
        interval = options["interval"]

        scheduler = Scheduler(queue_name=queue_name, connection=redis_conn, interval=interval)
        self.stdout.write(
            self.style.NOTICE(
                f"Starting RQ scheduler for queue '{queue_name}' (interval={interval}s)",
            ),
        )
        try:
            scheduler.run()
        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING("Scheduler stopped by user"))
