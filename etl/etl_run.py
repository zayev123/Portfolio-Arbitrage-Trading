from etl.data_extractor import DataExtractor
from etl.data_transformer import DataTransformer


class ETLRun:
    def __init__(self, stop_event, pairs_limit):
        self.extractor = DataExtractor(stop_event)
        self.transformer = DataTransformer(self.extractor.symbols_data)
        pairs_list = list(self.transformer.combinations_dict.keys())
        self.chosen_pairs = pairs_list[0:pairs_limit]

    def start_threads(self):
        self.extractor.build_sockets(self.chosen_pairs)

    def stop_threads(self):
        parent_pairs = self.extractor.pair_sockets
        for parent_pair, sub_pairs in  parent_pairs.items():
            for sub_pair, thread in sub_pairs.items():
                thread.stop()
                thread.join()
                print(f"stopping {sub_pair}")
            print("")

