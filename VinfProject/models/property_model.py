from dataclasses import dataclass, asdict


@dataclass
class PropertyModel:
    title: str = "NaN"
    property_type: str = "NaN"
    offer_type: str = "NaN"
    address: str = "NaN"
    area: str = "NaN"
    rooms: str = "NaN"
    energy_class: str = "NaN"
    year_built: str = "NaN"
    condition: str = "NaN"
    city: str = "NaN"
    price: str = "NaN"
    price_per_m2: str = "NaN"

    rent_price: str = "NaN"
    monthly_costs: str = "NaN"
    deposit: str = "NaN"
    availability: str = "NaN"
    rental_term: str = "NaN"

    land_area: str = "NaN"

    source_country: str = "NaN"
    source_url: str = "NaN"
