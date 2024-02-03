from sqlalchemy import Column, Integer, String, Float, ForeignKey

from db_constants import Base


class ParentPair(Base):
    __tablename__ = 'parent_pairs'
    id = Column(Integer, primary_key=True)
    symbol = Column(String, unique=False, nullable=False)

class ChildPair(Base):
    __tablename__ = 'child_pairs'
    id = Column(Integer, primary_key=True)
    symbol = Column(String, unique=False, nullable=False)
    parent_pair_id = Column(Integer, ForeignKey('parent_pairs.id'), nullable=False)
    best_bid_price = Column(Float, nullable=True)
    best_ask_price = Column(Float, nullable=True)