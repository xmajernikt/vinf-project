from dataclasses import dataclass

@dataclass
class FlatModel:
    title: str = ""
    price: str = ""
    area: str = ""
    rooms: str = ""
    floor: str = ""
    total_floors: str = ""
    address: str = ""
    description: str = ""
    energy_class: str = ""
    year_built: str = ""
    property_type: str = "flat"
