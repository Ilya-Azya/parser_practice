from sqlalchemy import Column, Integer, String, Float, Date, DateTime, func
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class SpimexTradingResult(Base):
    __tablename__ = 'spimex_trading_results'

    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange_product_id = Column(String)
    exchange_product_name = Column(String)
    oil_id = Column(String(4))
    delivery_basis_id = Column(String(3))
    delivery_basis_name = Column(String)
    delivery_type_id = Column(String(1))
    volume = Column(Float)
    total = Column(Float)
    count = Column(Integer)
    date = Column(Date)
    created_on = Column(DateTime, default=func.now())
    updated_on = Column(DateTime, default=func.now(), onupdate=func.now())
