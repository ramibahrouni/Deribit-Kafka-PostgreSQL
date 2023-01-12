

from dataclasses_avroschema import AvroModel



class Message():

    def __init__(self,
                 underlying_price: float,
                 timestamp: int,
                 settlement_price: float,
                 open_interest: float,
                 min_price: float,
                 max_price: float,
                 mark_price: float,
                 mark_iv: float,
                 last_price: float,
                 interest_rate: float,
                 instrument_name: str,
                 index_price: float,
                 bid_iv: float,
                 best_bid_price: float,
                 best_bid_amount: float,
                 best_ask_price: float,
                 best_ask_amount: float,
                 ask_iv: float):
        self.underlying_price = underlying_price
        self.timestamp = timestamp
        self.settlement_price = settlement_price
        self.open_interest = open_interest
        self.min_price = min_price
        self.max_price = max_price
        self.mark_price = mark_price
        self.mark_iv = mark_iv
        self.last_price = last_price
        self.interest_rate = interest_rate
        self.instrument_name = instrument_name
        self.index_price = index_price
        self.bid_iv = bid_iv
        self.best_bid_price = best_bid_price
        self.best_bid_amount = best_bid_amount
        self.best_ask_price = best_ask_price
        self.best_ask_amount = best_ask_amount
        self.ask_iv = ask_iv

    @staticmethod
    def from_json(json_dct):
        return Message(json_dct['underlying_price'],
                       json_dct['timestamp'],
                       json_dct['settlement_price'],
                       json_dct['open_interest'],
                       json_dct['min_price'],
                       json_dct['max_price'],
                       json_dct['mark_price'],
                       json_dct['mark_iv'],
                       json_dct['last_price'],
                       json_dct['interest_rate'],
                       json_dct['instrument_name'],
                       json_dct['index_price'],
                       json_dct['bid_iv'],
                       json_dct['best_bid_price'],
                       json_dct['best_bid_amount'],
                       json_dct['best_ask_price'],
                       json_dct['best_ask_amount'],
                       json_dct['ask_iv'])

    def to_json(self):
        return {
            "instrument_name": self.instrument_name,
            "underlying_price": self.underlying_price,
            "timestamp": self.timestamp,
            "settlement_price": self.settlement_price,
            "open_interest": self.open_interest,
            "min_price": self.min_price,
            "max_price": self.max_price,
            "mark_price": self.mark_price,
            "mark_iv": self.mark_iv,
            "last_price": self.last_price,
            "interest_rate": self.interest_rate,
            "index_price": self.index_price,
            "bid_iv": self.bid_iv,
            "best_bid_price": self.best_bid_price,
            "best_bid_amount": self.best_bid_amount,
            "best_ask_price": self.best_ask_price,
            "best_ask_amount": self.best_ask_amount,
            "ask_iv": self.ask_iv

        }

    @staticmethod
    def object_mapper(obj):
        return obj.to_json()
