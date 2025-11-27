from dataclasses import dataclass
from house_model import HouseModel

@dataclass
class HouseBuyModel(HouseModel):
    price_per_m2: str = ""          # Price per square meter
    ownership: str = ""             # Ownership type (private, cooperative, etc.)
    condition: str = ""             # Condition (new, renovated, etc.)

    def __init__(self):
        super().__init__()
