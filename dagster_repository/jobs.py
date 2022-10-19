from dagster import DailyPartitionsDefinition, daily_partitioned_config, define_asset_job, graph
from loguru import logger

from dagster_repository.ops import get_etf_infos, perform_analysis, update_database


@graph()
def sync_etf_pipeline():
    logger.info("Start collecting ETF info")

    update_database(
        get_etf_infos())


@graph
def analysis_etf_pipeline():
    perform_analysis()


@daily_partitioned_config(start_date="2022-10-01", timezone="Asia/Bangkok")
def materialize_ticker_job_config(start, end_):
    return {
        "ops": {
            "ticker": {
                "config": {
                    "tickers": ["XT"],
                    "date": start.strftime("%Y%m%d")
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


materialize_ticker_job = define_asset_job(
    name="materialize_ticker",
    selection="ticker",
    partitions_def=DailyPartitionsDefinition(
        start_date="2022-10-01", timezone="Asia/Bangkok"
    ),
    config=materialize_ticker_job_config,
)


@daily_partitioned_config(start_date="2022-10-01", timezone="Asia/Bangkok")
def materialize_ticker_meta_job_config(start, end_):
    return {
        "ops": {
            "ticker_meta": {
                "config": {
                    "ticker": "XT",
                    "date": start.strftime("%Y%m%d")
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


materialize_ticker_meta_job = define_asset_job(
    name="materialize_ticker_meta",
    selection="ticker_meta",
    partitions_def=DailyPartitionsDefinition(
        start_date="2022-10-01", timezone="Asia/Bangkok"
    ),
    config=materialize_ticker_meta_job_config,
)
