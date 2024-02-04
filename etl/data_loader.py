import time
from sqlalchemy import create_engine
from db_constants import Base
from sqlalchemy import func
import pandas as pd
from models import ParentPair, ChildPair
from sqlalchemy.orm import sessionmaker
from sqlalchemy import String, case, cast, literal_column
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
        qry = self.session.query(ParentPair)
        qry = qry.outerjoin(ChildPair, ChildPair.parent_pair_id == ParentPair.id)
        qry = qry.with_entities(
            ParentPair.symbol.label("parent_pair"),
            ChildPair.symbol.label("sub_pair"),
            ChildPair.best_ask_price.label("ask_price"),
            ChildPair.best_bid_price.label("bid_price")
        )
        data = qry.all()

        data_dict = {}

        for (parent_pair, sub_pair, ask_price, bid_price) in data:
            # print("(parent_pair, sub_pair, ask_price, bid_price)", f"({parent_pair}, {sub_pair}, {ask_price}, {bid_price})")
            sub_assets = parent_pair.split("-")
            if parent_pair not in data_dict:
                data_dict[parent_pair] = {}
                parent_pair: str = parent_pair
                parent_dict = data_dict[parent_pair]
                if len(sub_assets)>=3:
                    sub_pair_1 = f"{sub_assets[0]}{sub_assets[1]}"
                    sub_pair_2 = f"{sub_assets[0]}{sub_assets[2]}"
                    sub_pair_3 = f"{sub_assets[1]}{sub_assets[2]}"
                    parent_dict[sub_pair_1] = {}
                    parent_dict[sub_pair_2] = {}
                    parent_dict[sub_pair_3] = {}
            parent_dict = data_dict[parent_pair]
            if len(sub_assets)>=3:
                parent_dict[sub_pair] = {
                    "bid_price": bid_price,
                    "ask_price": ask_price
                }
            else:
                continue

        data_list = []
        for parent_key, children in data_dict.items():
            data_row = {}
            data_row["parent_pair"] = parent_key
            i = 1
            for sub_key, sub_data in children.items():
                bid_price = sub_data.get("bid_price", None)
                ask_price = sub_data.get("ask_price", None)
                if not bid_price or not ask_price:
                    continue
                data_row[f"sub_pair_{i}"] = sub_key
                data_row[f"bid_price_{i}"] = bid_price
                data_row[f"ask_price_{i}"] = ask_price
                i +=1
            if i > 3:
                data_list.append(data_row)
        
        return data_list