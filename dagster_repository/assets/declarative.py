from dagster import (
    AssetKey,
    DailyPartitionsDefinition,
    asset,
    Array,
    asset_sensor,
    build_schedule_from_partitioned_job,
    daily_partitioned_config,
    define_asset_job,
)

from dagster_repository.imperative import (
    Ticker,
    TickerMeta,
    get_etf_info,
)
from loguru import logger
from sqlmodel import create_engine, Session, SQLModel

from typing import List

from loguru import logger
from sqlalchemy import desc, func, select as sel
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


@daily_partitioned_config(start_date="2022-10-01", timezone="Asia/Bangkok")
def partition_config(start, end_):
    return {
        "ops": {
            "ticker": {"config": {"tickers": ["XT"], "date": start.strftime("%Y%m%d")}}
        }
    }


materialize_ticker_job = define_asset_job(
    name="materialize_ticker",
    selection="ticker",
    partitions_def=DailyPartitionsDefinition(
        start_date="2022-10-01", timezone="Asia/Bangkok"
    ),
    config=partition_config,
)

materialize_ticker_job_daily_schedule = build_schedule_from_partitioned_job(
    job=materialize_ticker_job,
    name="materialize_ticker_job_daily_schedule",
    hour_of_day=9,
    minute_of_hour=0,
)


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


@daily_partitioned_config(start_date="2022-10-01", timezone="Asia/Bangkok")
def partition_config2(start, end_):
    return {
        "ops": {
            "ticker_meta": {
                "config": {"ticker": "XT", "date": start.strftime("%Y%m%d")}
            }
        }
    }


materialize_ticker_meta_job = define_asset_job(
    name="materialize_ticker_meta",
    selection="ticker_meta",
    partitions_def=DailyPartitionsDefinition(
        start_date="2022-10-01", timezone="Asia/Bangkok"
    ),
    config=partition_config2,
)


@asset_sensor(
    name=f"on_ticker_update_sensor",
    asset_key=AssetKey("ticker"),
    job=materialize_ticker_meta_job,
    minimum_interval_seconds=10,
)
def on_ticker_update_sensor(context, asset_event):
    yield materialize_ticker_meta_job.run_request_for_partition(
        partition_key=asset_event.dagster_event.partition,
        run_key=asset_event.dagster_event.partition,
    )
