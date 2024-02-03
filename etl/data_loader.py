import time
from sqlalchemy import create_engine
from db_constants import Base
from sqlalchemy import func
import pandas as pd
from models import ParentPair, ChildPair
from sqlalchemy.orm import sessionmaker
from sqlalchemy import String
import queue

class DataLoader:
    def __init__(self):
        engine = create_engine('sqlite:///Cryptos.db', echo=False)
        Session = sessionmaker(bind=engine)
        self.session = Session()
        self.queue = queue.Queue()
        self.time_to_stop = None

    
    def queue_init_data(self, parent_pair, symbol):
        item = {
            "type": "init",
            "parent_pair": parent_pair,
            "symbol": symbol
        }
        self.queue.put(item)

    def get_init_data(self, parent_pair, symbol):
        parent_pair_obj = self.session.query(ParentPair).filter(ParentPair.symbol==parent_pair).first()
        
        child_pair_obj = self.session.query(ChildPair).filter(
            (ChildPair.symbol==symbol)
            & (ChildPair.parent_pair_id==parent_pair_obj.id)
        ).first()
        return (parent_pair, child_pair_obj)
    
    def store_init_data(self, parent_pair, symbol):
        parent_pair_obj = self.session.query(ParentPair).filter(ParentPair.symbol==parent_pair).first()
        if not parent_pair_obj:
            parent_pair_obj = ParentPair(symbol=parent_pair)
            self.session.add(parent_pair_obj)
            self.session.commit()
            parent_pair_obj = self.session.query(ParentPair).filter(ParentPair.symbol==parent_pair).first()
        parent_pair_obj = parent_pair_obj
        
        child_pair_obj = self.session.query(ChildPair).filter(
            (ChildPair.symbol==symbol)
            & (ChildPair.parent_pair_id==parent_pair_obj.id)
        ).first()
        if not child_pair_obj:
            child_pair_obj = ChildPair(
                symbol=symbol,
                parent_pair_id = parent_pair_obj.id,
            )
            self.session.add(child_pair_obj)
            self.session.commit()
            child_pair_obj = self.session.query(ChildPair).filter(
                (ChildPair.symbol==symbol)
                & (ChildPair.parent_pair_id==parent_pair_obj.id)
            ).first()
        child_pair_obj = child_pair_obj
        return child_pair_obj

    def queue_latest_data(self, parent_pair, symbol, update):
        item = {
            "type": "update",
            "parent_pair": parent_pair,
            "symbol": symbol,
            "update": update,
        }
        self.queue.put(item)

    def store_latest_data(self, parent_pair, symbol, update):
        best_bid_price = float(update['b'])
        best_ask_price = float(update['a'])
        
        child_pair_obj = self.store_init_data(parent_pair, symbol)

        if child_pair_obj:
            child_pair_obj.best_bid_price = best_bid_price
            child_pair_obj.best_ask_price = best_ask_price
            self.session.add(child_pair_obj)
            self.session.commit()

    def process_items(self):
        while True:
            item: dict = self.queue.get()
            if self.time_to_stop is not None:
                print("time_to_stop", self.queue.qsize())
            
            item_type = item.get("type", None)
            
            if item_type == "init":
                parent_pair = item.get("parent_pair", None)
                symbol = item.get("symbol", None)
                self.get_init_data(parent_pair, symbol)
                self.store_init_data(parent_pair=parent_pair, symbol=symbol)
            
            elif item_type == "update":
                update = item.get("update", {})
                parent_pair = item.get("parent_pair", None)
                symbol = item.get("symbol", None)
                self.store_latest_data(parent_pair=parent_pair, symbol=symbol,update=update)
            
            elif item_type == "stop":
                break
            
            self.queue.task_done()

    def stop_queue_process(self):
        item = {
            "type": "stop",
        }
        self.time_to_stop = self.queue.qsize()
        self.queue.put(item)
    
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