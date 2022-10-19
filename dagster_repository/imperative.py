from typing import Any, Dict, List
from dagster import (
    build_schedule_from_partitioned_job,
    daily_partitioned_config,
    graph,
    op,
    Array,
)

from loguru import logger
import yfinance as yf

from dagster_repository.models import Ticker
from dagster_repository.resources import sqlite_resource


def get_etf_info(symbol: str) -> Ticker:
    query = yf.Ticker(symbol)
    info = query.info
    logger.info(f"Query Ticker for symbol={symbol} success")

    name = info["longName"]
    quote_type = info["quoteType"]
    symbol = info["symbol"]
    nav_price = info["navPrice"]
    regular_market_price = info["regularMarketPrice"]
    regular_market_open = info["regularMarketOpen"]
    day_high = info["dayHigh"]
    day_low = info["dayLow"]
    previous_close = info["previousClose"]

    ticker = Ticker(
        name=name,
        quote_type=quote_type,
        symbol=symbol,
        nav_price=nav_price,
        market_price=regular_market_price,
        market_open=regular_market_open,
        day_high=day_high,
        day_low=day_low,
        previous_close=previous_close,
    )
    return ticker


@op(
    config_schema={"tickers": Array(str), "date": str},
    description="fetch ETF from sources",
)
def get_etf_infos(context) -> List[Ticker]:
    tickers = context.op_config["tickers"]
    logger.debug(f"ETF holding={tickers}")

    assets = [get_etf_info(item) for item in tickers]
    return assets


@op(
    description="Update Ticker to database",
    required_resource_keys={'db'}
)
def update_database(context, assets: List[Ticker]) -> Dict[str, Any]:
    return context.resources.db.add_assets(assets)


@graph()
def sync_etf_pipeline():
    logger.info("Start collecting ETF info")

    update_database(
        get_etf_infos())


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

# add more pipeline perform aggregate
# of market_price (max, min, avg) all time


@op(
    description="Read the etf history from DB",
    config_schema={"ticker": str, "date": str},
    required_resource_keys={'db'}
)
def perform_analysis(context):
    context.resources.db.analysis(
        context.op_config['ticker'],
        context.op_config['date'])


@graph
def analysis_etf_pipeline():
    perform_analysis()


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
