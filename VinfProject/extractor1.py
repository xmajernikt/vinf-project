import os
from dataclasses import asdict
from models.property_model import PropertyModel
from extraction_functions import *

class ExtractorSpark:

    @staticmethod
    def extract_document(file_tuple):
        """file_tuple = (path, html) from Spark wholeTextFiles"""

        path, html = file_tuple
        country = detect_country(html)
        offer_type = detect_offer_type(html, country.lower())

        price = extract_price(html, country) if offer_type == "buy" else "NaN"
        rent_price = extract_price(html, country) if offer_type == "rent" else "NaN"
        address = extract_address(html, country)
        model = PropertyModel(
            title=extract_field(html, r"<title>(.*?)</title>"),
            property_type=detect_property_type(html),
            offer_type=offer_type,
            address=extract_address(html, country),
            area=extract_field(html, r"([\d.,]+)\s*(?:m²|m2)"),
            rooms=extract_field(html, r"([0-9]+)\s*(?:Zimmer|izb|rooms?|kk)"),
            energy_class=extract_energy_class(html),
            year_built=extract_field(html, r"(?:Baujahr|Rok.*výstavby|Year built).*?([12][0-9]{3})"),
            condition=extract_field(html, r"(Novostavba|Pôvodný stav|Kompletná rekonštrukcia|Čiastočná rekonštrukcia|Neubau|Altbau|Renoviert)"),
            city=extract_city(address, country),
            price=price,
            price_per_m2=extract_price_per_m2(html, country),
            rent_price=rent_price,
            monthly_costs=extract_field(html, r"(?:Nebenkosten|Mesačné náklady).*?([0-9\s.,]+)"),
            deposit=extract_field(html, r"(?:Kaution|Depozit).*?([0-9\s.,]+)"),
            availability=extract_availability(html),
            rental_term=extract_field(html, r"(?:Mietdauer|Doba nájmu).*?([^<\n\r]+)"),
            land_area=extract_field(html, r"(?:Grundstücksfläche|Pozemok|Gartenfläche).*?([0-9.,]+)"),
            source_country=country,
            source_url=os.path.basename(path)
        )

        return asdict(model)