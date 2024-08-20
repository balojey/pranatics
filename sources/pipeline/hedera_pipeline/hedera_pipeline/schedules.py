from dagster import ScheduleDefinition
from .jobs import pipeline


pipeline_schedule = ScheduleDefinition(
    job=pipeline,
    cron_schedule="*/3 * * * *"
)