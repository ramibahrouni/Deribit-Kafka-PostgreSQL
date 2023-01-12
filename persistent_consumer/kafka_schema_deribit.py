import dataclasses
from dataclasses_avroschema import AvroModel


@dataclasses.dataclass
class Deribit(AvroModel):
    underlying_price: float
    timestamp: int
    settlement_price: float
    open_interest: float
    min_price: float
    max_price: float
    mark_price: float
    mark_iv: float
    last_price: float
    interest_rate: float
    instrument_name: str
    index_price: float
    bid_iv: float
    best_bid_price: float
    best_bid_amount: float
    best_ask_price: float
    best_ask_amount: float
    ask_iv: float
    currency: str
    maturity: str
    strike: str
    type: str

    class Meta:
        namespace = "Deribit.v1"
        aliases = ["DeribitV1", "deribit"]
