from typing import Any, Dict, List
from dagster import (
    op,
    Array,
)

from loguru import logger
import yfinance as yf

from dagster_repository.models import Ticker


def get_etf_info(symbol: str) -> Ticker:
    query = yf.Ticker(symbol)
    info = query.info
    logger.info(f"Query Ticker for symbol={symbol} success")

    ticker = Ticker(
        name=info["longName"],
        quote_type=info["quoteType"],
        symbol=info["symbol"],
        nav_price=info["navPrice"],
        market_price=info["regularMarketPrice"],
        market_open=info["regularMarketOpen"],
        day_high=info["dayHigh"],
        day_low=info["dayLow"],
        previous_close=info["previousClose"],
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


@op(
    description="Read the etf history from DB",
    config_schema={"ticker": str, "date": str},
    required_resource_keys={'db'}
)
def perform_analysis(context):
    context.resources.db.analysis(
        context.op_config['ticker'],
        context.op_config['date'])
