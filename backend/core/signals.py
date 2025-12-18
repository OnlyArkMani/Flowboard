from __future__ import annotations

from django.db.models.signals import post_delete, post_save
from django.dispatch import receiver

from .models import Job
from .scheduler import register_cron_schedule, cancel_cron_schedule


@receiver(post_save, sender=Job)
def sync_job_schedule(sender, instance: Job, **kwargs):
  register_cron_schedule(instance)


@receiver(post_delete, sender=Job)
def remove_job_schedule(sender, instance: Job, **kwargs):
  cancel_cron_schedule(instance.id)
