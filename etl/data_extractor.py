import requests

from socket_builder import WebSocketThread

class DataExtractor:
    def __init__(self, stop_event):
        self.symbols_data = None
        self.get_symbols()
        self.pair_sockets: dict[str, dict[str, WebSocketThread]] = {}
        self.base_sckt = "wss://stream.binance.com:9443/ws"
        self.stop_event = stop_event

    def get_symbols(self):
        url = "https://api.binance.com/api/v3/exchangeInfo"
        # Sending a GET request
        response = requests.get(url)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            exchange_info = response.json()
            symbols_data = exchange_info['symbols']
            self.symbols_data = symbols_data
        else:
            print(f"Error: {response.status_code} - {response.text}")

    def build_sockets(self, pairs_list: list[str]):
        for pairing in pairs_list:
            if pairing not in self.pair_sockets:
                self.pair_sockets[pairing] = {}
            parent_pair = self.pair_sockets[pairing]
            sub_symbols = pairing.split("-")
            if len(sub_symbols) != 3:
                continue
            pair_A = f"{sub_symbols[0]}{sub_symbols[1]}"
            pair_B = f"{sub_symbols[0]}{sub_symbols[2]}"
            pair_C = f"{sub_symbols[1]}{sub_symbols[2]}"
            sub_pairs_list = [pair_A, pair_B, pair_C]
            for sub_pair in sub_pairs_list:
                endpoint = f"{self.base_sckt}/{sub_pair.lower()}@bookTicker"
                sub_thread = WebSocketThread(endpoint, sub_pair, pairing, data_loader=None)
                sub_thread.start()
                parent_pair[sub_pair] = sub_thread
