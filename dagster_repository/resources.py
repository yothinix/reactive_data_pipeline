from typing import Any, Dict, List

from dagster import StringSource, resource
from loguru import logger
from sqlalchemy import desc, func, select as sel
from sqlmodel import create_engine, select, Session, SQLModel

from dagster_repository.models import Ticker


@resource(config_schema={'connection_url': StringSource})
def sqlite_resource(context):
    return SQLiteResource(**context.resource_config)


class SQLiteResource:
    connection_url: str

    def __init__(self, connection_url: str):
        self.connection_url = connection_url

    def get_engine(self):
        engine = create_engine(self.connection_url)
        SQLModel.metadata.create_all(engine)
        logger.info(f"Create database connection success at {self.connection_url}")
        return engine

    def add_assets(self, assets: List[Ticker]) -> Dict[str, Any]:
        engine = self.get_engine()
        with Session(engine) as session:
            for asset in assets:
                session.add(asset)
                logger.info(
                    f"Added {asset.symbol} NAV:{asset.nav_price} PRICE:{asset.market_price} at {asset.created_at}"
                )

            session.commit()
            logger.info("Write to database success")

            metadata = self._get_result_metadata(session)
            return metadata

    def _get_result_metadata(self, session: Session) -> Dict[str, Any]:
        result_raw_count = session.exec(sel(func.count(Ticker.id))).scalar_one()

        query = select(Ticker).order_by(desc(Ticker.created_at)).limit(5)
        results = session.exec(query)

        metadata = {
            "count": result_raw_count,
            "last_5_row": [ticker.dict() for ticker in results],
        }
        return metadata
