from datetime import datetime
from typing import Optional

from sqlmodel import Field, SQLModel


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
