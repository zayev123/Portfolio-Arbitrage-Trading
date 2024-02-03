from sqlalchemy import create_engine
from db_constants import Base
from sqlalchemy import func
import pandas as pd
from models import ParentPair, ChildPair
from sqlalchemy.orm import sessionmaker
from sqlalchemy import String

class DataLoader:
    def __init__(self):
        engine = create_engine('sqlite:///Cryptos.db', echo=False)
        Session = sessionmaker(bind=engine)
        self.session = Session()
    
    def get_latest_data(self):
        self.session.commit()
        subq = (
            self.session.query(
                ParentPair.id.label('parent_id'),
                func.group_concat(ChildPair.symbol.cast(String), ', ').label('sub_pairs_list'),
                func.group_concat(ChildPair.best_bid_price.cast(String), ', ').label('best_bid_prices_list'),
                func.group_concat(ChildPair.best_ask_price.cast(String), ', ').label('best_ask_prices_list')
            )
            .outerjoin(ChildPair, ChildPair.parent_pair_id == ParentPair.id)
            .group_by(ParentPair.id)
            .subquery()
        )

        # Main query
        qry = (
            self.session.query(
                ParentPair.id,
                ParentPair.symbol.label("assets"),
                subq.c.sub_pairs_list,
                subq.c.best_bid_prices_list,
                subq.c.best_ask_prices_list,
            )
            .join(subq, subq.c.parent_id == ParentPair.id)
        )

        result = qry.all()
        return result