import threading
import websocket
import json
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from etl.data_loader import DataLoader

from models import ChildPair, ParentPair

class WebSocketThread(threading.Thread):
    def __init__(self, endpoint, symbol, parent_pair, data_loader: DataLoader):
        super().__init__()
        self.ws = websocket.WebSocketApp(
            endpoint,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        self.symbol = symbol
        self.parent_pair = parent_pair
        self.updates = []

        # Create SQLite database and tables
        engine = create_engine('sqlite:///Cryptos.db', echo=False, connect_args={'check_same_thread': False})
        Session = sessionmaker(bind=engine)
        self.session = Session()
        if not data_loader:
            self.data_loader = DataLoader()
        else:
            self.data_loader = data_loader
        self.data_loader.store_init_data(self.parent_pair, self.symbol)

        # Insert parent pair into the parent_pairs table
        # print("my_parent_pair", parent_pair)
        
    # def store_init_data(self):
    #     parent_pair_obj = self.session.query(ParentPair).filter(ParentPair.symbol==self.parent_pair).first()
    #     if not parent_pair_obj:
    #         parent_pair_obj = ParentPair(symbol=self.parent_pair)
    #         self.session.add(parent_pair_obj)
    #         self.session.commit()
    #         parent_pair_obj = self.session.query(ParentPair).filter(ParentPair.symbol==self.parent_pair).first()
    #     self.parent_pair_obj = parent_pair_obj
        
    #     child_pair_obj = self.session.query(ChildPair).filter(
    #         (ChildPair.symbol==self.symbol)
    #         & (ChildPair.parent_pair_id==parent_pair_obj.id)
    #     ).first()
    #     if not child_pair_obj:
    #         child_pair_obj = ChildPair(
    #             symbol=self.symbol,
    #             parent_pair_id = parent_pair_obj.id,
    #         )
    #         self.session.add(child_pair_obj)
    #         self.session.commit()
    #         child_pair_obj = self.session.query(ChildPair).filter(
    #             (ChildPair.symbol==self.symbol)
    #             & (ChildPair.parent_pair_id==parent_pair_obj.id)
    #         ).first()
    #     self.child_pair_obj = child_pair_obj

    def on_open(self, ws):
        print(f"Connected {self.symbol}")

    def on_message(self, ws, message):
        update = json.loads(message)
        self.data_loader.store_latest_data(self.parent_pair, self.symbol, update)
        # self.store_data(update)

    # def store_data(self, update):

    #     # Extract data from the message
    #     best_bid_price = float(update['b'])
    #     best_ask_price = float(update['a'])

    #     if self.child_pair_obj:
    #         self.child_pair_obj.best_bid_price = best_bid_price
    #         self.child_pair_obj.best_ask_price = best_ask_price
    #         self.session.add(self.child_pair_obj)
    #         self.session.commit()


    def on_error(self, ws, error):
        print(f"Error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        print(f"Connection close: {close_status_code} - {close_msg}")
        self.session.close()

    def run(self):
        self.ws.run_forever()

    def stop(self):
        self.ws.close()
