import time
from redis import StrictRedis
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
        # engine = create_engine('sqlite:///Cryptos.db', echo=False)
        # Session = sessionmaker(bind=engine)
        # self.session = Session()
        # self.queue = queue.Queue()
        self.time_to_stop = None
        redis_host = "localhost"
        redis_port = 6379
        self.redis_memory = StrictRedis(
            host=redis_host,
            port=redis_port,
            charset="utf-8",
            decode_responses=True,
        )

    
    # def queue_init_data(self, parent_pair, symbol):
    #     item = {
    #         "type": "init",
    #         "parent_pair": parent_pair,
    #         "symbol": symbol
    #     }
    #     self.queue.put(item)
    
    def store_init_data(self, parent_pair, symbol):
        red_mem = self.redis_memory
        curr_parent_dta: dict = red_mem.hgetall(parent_pair)
        if not curr_parent_dta or symbol not in curr_parent_dta:
            red_mem.hset(parent_pair, symbol, symbol)
            curr_sym_data = red_mem.hgetall(symbol)
            if not curr_sym_data:
                red_mem.hset(symbol, "bid_price", 0)
                red_mem.hset(symbol, "ask_price", 0)

        red_mem.hset("parent_pairs", parent_pair, parent_pair)

            
    # def queue_latest_data(self, parent_pair, symbol, update):
    #     item = {
    #         "type": "update",
    #         "parent_pair": parent_pair,
    #         "symbol": symbol,
    #         "update": update,
    #     }
    #     self.queue.put(item)

    def store_latest_data(self, parent_pair, symbol, update):
        best_bid_price = float(update['b'])
        best_ask_price = float(update['a'])
        
        self.store_init_data(parent_pair, symbol)
        red_mem = self.redis_memory

        red_mem.hset(symbol, "bid_price", best_bid_price)
        red_mem.hset(symbol, "ask_price", best_ask_price)

    # def process_items(self):
    #     while True:
    #         item: dict = self.queue.get()
    #         if self.time_to_stop is not None:
    #             print("time_to_stop", self.queue.qsize())
            
    #         item_type = item.get("type", None)
            
    #         if item_type == "init":
    #             parent_pair = item.get("parent_pair", None)
    #             symbol = item.get("symbol", None)
    #             self.store_init_data(parent_pair=parent_pair, symbol=symbol)
            
    #         elif item_type == "update":
    #             update = item.get("update", {})
    #             parent_pair = item.get("parent_pair", None)
    #             symbol = item.get("symbol", None)
    #             self.store_latest_data(parent_pair=parent_pair, symbol=symbol,update=update)
            
    #         elif item_type == "stop":
    #             break
            
    #         self.queue.task_done()

    # def stop_queue_process(self):
    #     item = {
    #         "type": "stop",
    #     }
    #     self.time_to_stop = self.queue.qsize()
    #     self.queue.put(item)
    
    def get_latest_data(self):
        data_dicts = {}
        red_mem = self.redis_memory
        parent_pairs: dict = red_mem.hgetall("parent_pairs")
        for prnt_pair in parent_pairs.keys():
            if prnt_pair not in data_dicts:
                data_dicts[prnt_pair] = {}
            parent_dict = data_dicts[prnt_pair]
            child_pairs: dict = red_mem.hgetall(prnt_pair)
            for child_pair in child_pairs.keys():
                if child_pair not in parent_dict:
                    parent_dict[child_pair] = {}
                child_data: dict = red_mem.hgetall(child_pair)
                parent_dict[child_pair] = child_data
        
        data_list = []
        for parent_key, children in data_dicts.items():
            data_row = {}
            data_row["parent_pair"] = parent_key
            i = 1
            for child_key, child_data in children.items():
                data_row[f"sub_pair_{i}"] = child_key
                data_row[f"bid_price_{i}"] = child_data.get("bid_price", None)
                data_row[f"ask_price_{i}"] = child_data.get("ask_price", None)
                i=i+1
            if i >3:
                data_list.append(data_row)

        return data_list