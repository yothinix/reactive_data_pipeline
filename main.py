from datetime import datetime
from typing import Any, Dict, List, Optional

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


def get_etf_infos(tickers: List[str]) -> List[Ticker]:
    assets = [get_etf_info(item) for item in tickers]
    # logger.info(f'assets={assets}')
    return assets


def get_result_metadata(session: Session) -> Dict[str, Any]:
    result_raw_count = session.exec(sel(func.count(Ticker.id))).scalar_one()

    query = select(Ticker).order_by(desc(Ticker.created_at)).limit(5)
    results = session.exec(query)

    metadata = {
        "count": result_raw_count,
        "last_5_row": [ticker.dict() for ticker in results],
    }
    # logger.info(metadata)
    return metadata


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


def main():
    logger.info("Start collecting ETF info")
    holding = ["XT", "SCHX", "IXJ", "WCLD"]
    logger.debug(f"ETF holding={holding}")

    update_database(get_etf_infos(holding))


if __name__ == "__main__":
    main()
