from dataclasses import dataclass
from house_model import HouseModel

@dataclass
class HouseRentModel(HouseModel):
    monthly_costs: str = ""         # Additional monthly fees (utilities, etc.)
    deposit: str = ""               # Security deposit
    availability: str = ""          # When available
    rental_term: str = ""           # Long-term / short-term

    def __init__(self):
        super().__init__()
