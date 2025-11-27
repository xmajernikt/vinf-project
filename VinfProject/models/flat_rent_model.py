from dataclasses import dataclass
from flat_model import FlatModel

@dataclass
class FlatRentModel(FlatModel):
    monthly_costs: str = ""
    deposit: str = ""
    availability: str = ""
    rental_term: str = ""

    def __init__(self):
        super().__init__()
