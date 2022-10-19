from dagster import (
    DailyPartitionsDefinition,
    asset,
    Array,
)

from dagster_repository.ops import (
    get_etf_info,
)
from dagster_repository.models import Ticker, TickerMeta

from typing import List

from loguru import logger
from sqlalchemy import func, select as sel
from sqlmodel import create_engine, select, Session, SQLModel


@asset(
    compute_kind="sqlite",
    config_schema={"tickers": Array(str), "date": str},
    partitions_def=DailyPartitionsDefinition(
        start_date="2022-10-01", timezone="Asia/Bangkok"
    ),
)
def ticker(context) -> List[Ticker]:
    tickers = context.op_config["tickers"]
    logger.debug(f"ETF holding={tickers}")

    assets = [get_etf_info(item) for item in tickers]

    connection_url = "sqlite:///database.sqlite"
    engine = create_engine(connection_url)
    SQLModel.metadata.create_all(engine)
    logger.info(f"Create database connection success at {connection_url}")

    with Session(engine) as session:
        for asset in assets:
            session.add(asset)
            logger.info(
                f"Added {asset.symbol} NAV:{asset.nav_price} PRICE:{asset.market_price} at {asset.created_at}"
            )

        session.commit()
        logger.info("Write to database success")

    query = select(Ticker)
    results = session.exec(query)
    return [result for result in results]


@asset(
    compute_kind="sqlite",
    config_schema={"ticker": str, "date": str},
    partitions_def=DailyPartitionsDefinition(
        start_date="2022-10-01", timezone="Asia/Bangkok"
    ),
)
def ticker_meta(context, ticker):
    connection_url = "sqlite:///database.sqlite"
    engine = create_engine(connection_url)
    SQLModel.metadata.create_all(engine)
    logger.info(f"Create database connection success at {connection_url}")

    with Session(engine) as session:
        max_market_price = session.exec(
            sel(func.max(Ticker.market_price)).where(
                Ticker.symbol == context.op_config["ticker"]
            )
        ).scalar_one()
        min_market_price = session.exec(
            sel(func.min(Ticker.market_price)).where(
                Ticker.symbol == context.op_config["ticker"]
            )
        ).scalar_one()

        ticker_meta = TickerMeta(
            symbol=context.op_config["ticker"],
            monthly_max_market_price=max_market_price,
            monthly_min_market_price=min_market_price,
            partition=context.op_config["date"],
        )
        session.add(ticker_meta)
        session.commit()
