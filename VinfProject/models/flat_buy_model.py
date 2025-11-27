from dataclasses import dataclass
from flat_model import FlatModel

@dataclass
class FlatBuyModel(FlatModel):
    price_per_m2: str = ""
    ownership: str = ""
    condition: str = ""

    def __init__(self):
        super().__init__()
