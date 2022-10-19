from dagster import build_schedule_from_partitioned_job, daily_partitioned_config

from dagster_repository.jobs import (
    analysis_etf_pipeline, materialize_ticker_job,
    sync_etf_pipeline
)
from dagster_repository.resources import sqlite_resource


@daily_partitioned_config(start_date="2022-10-01", timezone="Asia/Bangkok")
def partition_config(start, end_):
    return {
        "ops": {
            "get_etf_infos": {
                "config": {
                    "tickers": ["XT", "SCHX", "IXJ", "WCLD"],
                    "date": start.strftime("%Y%m%d"),
                }
            }
        },
        "resources": {
            "db": {
                "config": {
                    "connection_url": "sqlite:///database.sqlite"
                }
            }
        }
    }


sync_etf_pipeline_daily_schedule = build_schedule_from_partitioned_job(
    job=sync_etf_pipeline.to_job(
        name="sync_etf_pipeline_daily_schedule",
        config=partition_config,
        resource_defs={
            'db': sqlite_resource
        }
    ),
    name="sync_etf_pipeline_daily_schedule",
    hour_of_day=9,
    minute_of_hour=0,
)


@daily_partitioned_config(start_date="2022-10-01", timezone="Asia/Bangkok")
def analysis_partition_config(start, end_):
    return {
        "ops": {
            "perform_analysis": {
                "config": {"ticker": "XT", "date": start.strftime("%Y%m%d")}
            }
        },
        "resources": {
            "db": {
                "config": {
                    "connection_url": "sqlite:///database.sqlite"
                }
            }
        }
    }


analysis_etf_pipeline_daily_schedule = build_schedule_from_partitioned_job(
    job=analysis_etf_pipeline.to_job(
        name="analysis_etf_pipeline_daily_schedule",
        config=analysis_partition_config,
        resource_defs={
            'db': sqlite_resource
        }
    ),
    name="analysis_etf_pipeline_daily_schedule",
    hour_of_day=10,
    minute_of_hour=0,
)
materialize_ticker_job_daily_schedule = build_schedule_from_partitioned_job(
    job=materialize_ticker_job,
    name="materialize_ticker_job_daily_schedule",
    hour_of_day=9,
    minute_of_hour=0,
)
