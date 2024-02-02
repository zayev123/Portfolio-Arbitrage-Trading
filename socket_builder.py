import json
import threading

import websocket


class WebSocketThread(threading.Thread):
    def __init__(self, endpoint, symbol, parent_pair):
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
        self.updates = []  # List to store updates

    def on_open(self, ws):
        print(f"Connected {self.symbol}")

    def on_message(self, ws, message):
        update = json.loads(message)
        self.updates.append(update)
        # print(f"New update: {update}")
        # You can perform additional actions or processing here

    def on_error(self, ws, error):
        print(f"Error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        print(f"Connection close: {close_status_code} - {close_msg}")

    def run(self):
        self.ws.run_forever()

    def stop(self):
        self.ws.close()