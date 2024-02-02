class DataTransformer:

    def __init__(self, symbols_data):
        self.symbols_data = symbols_data
        self.symbols_dict = {}
        self.bases_dict = {}
        self.quotes_dict = {}
        self.combinations_dict = {}
        self.make_combinations()


    
    def make_combinations(self):
        for data in self.symbols_data:
            symbol = data["symbol"]
            baseAsset = data["baseAsset"]
            quoteAsset = data["quoteAsset"]
            if symbol not in self.symbols_dict:
                self.symbols_dict[symbol] = {
                    baseAsset: "base",
                    quoteAsset: "quote"
                }
            
            if baseAsset not in self.bases_dict:
                self.bases_dict[baseAsset] = {}
            
            base_assets_dict = self.bases_dict[baseAsset]
            if quoteAsset not in base_assets_dict:
                base_assets_dict[quoteAsset] = quoteAsset

            if quoteAsset not in self.quotes_dict:
                self.quotes_dict[quoteAsset] = {}
            
            quote_assets_dict = self.quotes_dict[quoteAsset]
            if baseAsset not in quote_assets_dict:
                quote_assets_dict[baseAsset] = baseAsset
        
        for base in self.bases_dict:
            base_quotes = self.bases_dict[base]
            for base_quote_a in base_quotes:
                quote_a = base_quote_a
                for base_quote_b in base_quotes:
                    if base_quote_b!=quote_a:
                        quote_b = base_quote_b
                        if quote_a in self.bases_dict:
                            quote_a_base_quotes = self.bases_dict[quote_a]
                            if quote_b in quote_a_base_quotes:
                                stringBs = f"{base}-{quote_a}-{quote_b}"
                                self.combinations_dict[stringBs] = {
                                    "base": base,
                                    "quote_a": quote_a,
                                    "quote_b": quote_b
                                }
                        
                        if quote_a in self.quotes_dict:
                            quote_a_base_quotes = self.quotes_dict[quote_a]
                            if quote_b in quote_a_base_quotes:
                                stringQt = f"{base}-{quote_a}-{quote_b}"
                                self.combinations_dict[stringQt] = {
                                    "base": base,
                                    "quote_a": quote_a,
                                    "quote_b": quote_b
                                }



