from datetime import datetime
from typing import Any, Dict, List, Optional
from dagster import (
    build_schedule_from_partitioned_job,
    daily_partitioned_config,
    graph,
    op,
    Array,
)

from loguru import logger
from sqlalchemy import desc, func, select as sel
from sqlmodel import create_engine, Field, select, Session, SQLModel
import yfinance as yf


class Ticker(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    quote_type: str
    symbol: str
    nav_price: float
    market_price: float
    market_open: float
    day_high: float
    day_low: float
    previous_close: float
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)


class TickerMeta(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    symbol: str
    monthly_max_market_price: float
    monthly_min_market_price: float
    partition: str


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


def get_result_metadata(session: Session) -> Dict[str, Any]:
    result_raw_count = session.exec(sel(func.count(Ticker.id))).scalar_one()

    query = select(Ticker).order_by(desc(Ticker.created_at)).limit(5)
    results = session.exec(query)

    metadata = {
        "count": result_raw_count,
        "last_5_row": [ticker.dict() for ticker in results],
    }
    return metadata


@op(description="Update Ticker to database")
def update_database(assets: List[Ticker]) -> Dict[str, Any]:

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

        metadata = get_result_metadata(session)
        return metadata


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
        }
    }


sync_etf_pipeline_daily_schedule = build_schedule_from_partitioned_job(
    job=sync_etf_pipeline.to_job(
        name="sync_etf_pipeline_daily_schedule", config=partition_config
    ),
    name="sync_etf_pipeline_daily_schedule",
    hour_of_day=9,
    minute_of_hour=0,
)

# add more pipeline perform aggregate
# of market_price (max, min, avg) all time


@op(
    config_schema={"ticker": str, "date": str},
    description="Read the etf history from DB",
)
def perform_analysis(context):
    ...
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


@graph
def analysis_etf_pipeline():
    perform_analysis()


@daily_partitioned_config(start_date="2022-10-01", timezone="Asia/Bangkok")
def partition_config2(start, end_):
    return {
        "ops": {
            "perform_analysis": {
                "config": {"ticker": "XT", "date": start.strftime("%Y%m%d")}
            }
        }
    }


analysis_etf_pipeline_daily_schedule = build_schedule_from_partitioned_job(
    job=analysis_etf_pipeline.to_job(
        name="analysis_etf_pipeline_daily_schedule", config=partition_config2
    ),
    name="analysis_etf_pipeline_daily_schedule",
    hour_of_day=10,
    minute_of_hour=0,
)
