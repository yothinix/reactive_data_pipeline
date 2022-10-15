from dagster import load_assets_from_package_module, repository

from dagster_repository import assets
from dagster_repository.assets.declarative import (
    materialize_ticker_job,
    materialize_ticker_job_daily_schedule,
    materialize_ticker_meta_job,
    on_ticker_update_sensor,
)
from dagster_repository.imperative import (
    sync_etf_pipeline_daily_schedule,
    analysis_etf_pipeline_daily_schedule,
)


@repository
def dagster_repository():
    jobs = [materialize_ticker_job, materialize_ticker_meta_job]
    schedules = [
        sync_etf_pipeline_daily_schedule,
        analysis_etf_pipeline_daily_schedule,
        materialize_ticker_job_daily_schedule,
    ]
    sensors = [on_ticker_update_sensor]
    return jobs + schedules + sensors + [load_assets_from_package_module(assets)]
