from dagster import (
    DailyPartitionsDefinition,
    asset,
    Array,
)

from dagster_repository.ops import (
    get_etf_info,
)
from dagster_repository.models import Ticker

from typing import List

from loguru import logger

from dagster_repository.resources import sqlite_resource


@asset(
    compute_kind="sqlite",
    config_schema={"tickers": Array(str), "date": str},
    partitions_def=DailyPartitionsDefinition(
        start_date="2022-10-01", timezone="Asia/Bangkok"
    ),
    required_resource_keys={'db'},
    resource_defs={'db': sqlite_resource}
)
def ticker(context) -> List[Ticker]:
    tickers = context.op_config["tickers"]
    logger.debug(f"ETF holding={tickers}")

    assets = [get_etf_info(item) for item in tickers]

    context.resources.db.add_assets(assets)
    return assets


@asset(
    compute_kind="sqlite",
    config_schema={"ticker": str, "date": str},
    partitions_def=DailyPartitionsDefinition(
        start_date="2022-10-01", timezone="Asia/Bangkok"
    ),
    required_resource_keys={'db'},
    resource_defs={'db': sqlite_resource}
)
def ticker_meta(context, ticker):
    context.resources.db.analysis(
        context.op_config['ticker'],
        context.op_config['date'])
