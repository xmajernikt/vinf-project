from dataclasses import dataclass

@dataclass
class HouseModel:
    title: str = ""                 # Listing title
    price: str = ""                 # Total or monthly price
    area: str = ""                  # Living area (m²)
    land_area: str = ""             # Plot size (m²)
    rooms: str = ""                 # Number of rooms
    address: str = ""               # Address / location
    description: str = ""           # Description text
    energy_class: str = ""          # Energy efficiency (if available)
    year_built: str = ""            # Construction year (if available)
    property_type: str = "house"    # Static type marker
