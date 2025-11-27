import re
import csv
import os
from dataclasses import asdict
from models.property_model import PropertyModel


class Extractor:
    def __init__(self, input_files: list[str], output_path: str):
        self.input_files = input_files
        self.output_path = output_path
        self.results = []
        self.country = ""
        self.path = ""
        self.counter = 0

    def _detect_country(self, html: str) -> str:
        html_lower = html.lower()
        if "nehnutelnosti.sk" in html_lower:
            return "Slovakia"
        elif "sreality.cz" in html_lower:
            return "Czechia"
        elif "immobilienscout24.at" in html_lower or "immowelt.at" in html_lower or "willhaben.at" in html_lower:
            return "Austria"
        return "Unknown"

    def _detect_property_type(self, html: str) -> str:
        html_lower = html.lower()
        if re.search(r"\b(byt|wohnung|flat|apartment|apartmán)\b", html_lower):
            return "flat"
        elif re.search(r"\b(dom|haus|family house|dům|rodinný dům|rodinný dom)\b", html_lower):
            return "house"
        return "unknown"

    def _detect_offer_type(self, html: str) -> str:
        html_lower = html.lower()
        if re.search(r"\b(miete|mieten|rent|na prenájom|for rent|pronájem|na pronájem)\b", html_lower):
            return "rent"
        elif re.search(r"\b(predaj|verkauf|kaufen|for sale|na predaj|prodej|na prodej)\b", html_lower):
            return "buy"
        return "unknown"

    def _extract_fields(self, html: str) -> dict:
        patterns = {
            "title": r"<title>(.*?)</title>",
            "address": r"(?:Adresse|Adresa|Adresa nehnuteľnosti|Lokalita|Ulice|Obec|Ort)\s*[:\-]?\s*([^<\n\r]+)",
            "area": r"(?:Wohnfläche|Gesamtfläche|Užitná\s*plocha|Plocha(?:\s*(?:bytu|domu))?|Nutzfläche|plocha)[\s\S]{0,300}?([\d,.]+)\s*(?:m²|m2)",
            "land_area": r"(?:Grundstücksfläche|Pozemok)\s*[:\-]?\s*([0-9]+[,\.]?[0-9]*)\s*m",
            "rooms": r"([0-9]+)\s*(?:Zimmer|izb|rooms?|kk)",
            "energy_class": r"(?:Energieausweis|Energetická trieda|Energy class)\s*[:\-]?\s*([A-G])",
            "year_built": r"""(?isx)
                        (?:Baujahr|Rok\s*výstavby|Year\s*built)      
                        (?:\s|&nbsp;|<!--.*?-->|<[^>]+>){0,10}       
                        ([12][0-9]{3})                               
                        """,
            # "condition": r"(?:Stav nehnuteľnosti|Zustand|Condition)\s*[:\-]?\s*([A-Za-zÀ-ž\s]+)",
            "price": r"Kaufpreis([0-9][0-9\s.,]*\s?(?:€|eur|kč))",
            "price_per_m2": r"([0-9][0-9\s.,]*\s?(?:€|eur|kč))\s*/\s*m²",
            "monthly_costs": r"(?:Nebenkosten|Mesačné náklady|Monthly costs)\s*[:\-]?\s*([0-9\s.,]+)",
            "deposit": r"(?:Kaution: €|Depozit)\s*[:\-]?\s*([0-9\s.,]+)",
            "availability": r"(?:Verfügbar ab|Voľný od|Available from)\s*[:\-]?\s*([^<\n\r]+)",
            "rental_term": r"(?:Mietdauer|Doba nájmu|Rental term)\s*[:\-]?\s*([^<\n\r]+)",
        }

        data = {}
        for key, pattern in patterns.items():
            m = re.search(pattern, html, re.IGNORECASE)
            data[key] = m.group(1).strip() if m else "NaN"
        return data

    def process_file(self, path: str):
        try:
            with open(path, encoding="utf-8") as f:
                html = f.read()
        except Exception as e:
            print(f"Failed to open {path}: {e}")
            return

        self.country = self._detect_country(html)
        property_type = self._detect_property_type(html)
        offer_type = self.detect_offer_type(html)
        fields = self._extract_fields(html)

        extracted_price_rent = self.extract_price(html)
        price = extracted_price_rent if offer_type == "buy" else "NaN"
        rent_price = extracted_price_rent if offer_type == "rent" else "NaN"

        model = PropertyModel(
            title=fields["title"],
            property_type=property_type,
            offer_type=offer_type,
            address=self.extract_address(html),
            area=fields["area"],
            rooms=fields["rooms"],
            energy_class=self._extract_energy_class(html),
            year_built=fields["year_built"],
            condition=self._extract_condition(html),
            price=price,
            price_per_m2=self.extract_price_per_m(html),
            rent_price=rent_price,
            monthly_costs=fields["monthly_costs"],
            deposit=fields["deposit"],
            availability=self.extract_availability(html),
            rental_term=fields["rental_term"],
            land_area=fields["land_area"],
            source_country=self.country,
            source_url=os.path.basename(path),
        )

        self.results.append(model)

    def run(self):
        counter = 0
        for path in self.input_files:
            self.path = path
            self.counter += 1
            self.process_file(path)
            counter += 1
            if counter % 10 == 0:
                print(f"{counter} / {len(self.input_files)} pages processed")
        self._save()

    def _save(self):
        if not self.results:
            print("No data extracted.")
            return
        with open(self.output_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=asdict(self.results[0]).keys(), delimiter="\t")
            writer.writeheader()
            for prop in self.results:
                writer.writerow(asdict(prop))
        print(f"✅ Saved {len(self.results)} records to {self.output_path}")

    def _extract_condition(self, html: str) -> str:

        match = re.search(
            r"(Novostavba|Pôvodný stav|Kompletná rekonštrukcia|Čiastočná rekonštrukcia|Neubau|Altbau|Renoviert)",
            html,
            re.IGNORECASE,
        )
        return match.group(1).strip() if match else "NaN"

    def _extract_energy_class(self, html: str) -> str:
        energy_regex = re.compile(
            r"""(?isx)
            <div[^>]*class\s*=\s*["']Indicator-indicator-aA6["'][^>]*>  
            .*?                                                         
            <span[^>]*>                                                 
            \s*                                                         
            (A\+\+|A\+|A0|A1|A2|A|B|C|D|E|F|G)                          
            \s*                                                         
            </span>                                                    
            """,
            re.UNICODE,
        )

        match = re.search(energy_regex, html)
        if match:
            return match.group(1).upper()
        match2 = re.search(r"\b(A\+\+|A\+|A0|A1|A2|A|B|C|D|E|F|G)\b", html)
        return match2.group(1).upper() if match2 else "NaN"

    def extract_price(self, html):
        if self.country == "Slovakia":
            price_regex = re.compile(
                r"""(?isx)
                <p[^>]*class\s*=\s*["']MuiTypography-root\s+MuiTypography-h3[^"']*["'][^>]*>  
                \s*([0-9\.\s,&nbsp;]+€)                                                       
                """,
                re.UNICODE,
            )
        else:
            price_regex = re.compile(
                r"""(?isx)
                <div[^>]*class\s*=\s*["']Costs-row-[^"']*["'][^>]*>    
                [^<]*?<span[^>]*>                                       
                \s*(?:Kaufpreis|Miete|Monatliche(?:\s|&nbsp;|<!--.*?-->)*Kosten)\s* 
                </span>                                                 
                .*?                                                     
                <span[^>]*class\s*=\s*["']Costs-price-[^"']*["'][^>]*>  
                \s*([0-9.,\s]+€)                                      
                \s*</span>                                              
                """,
                re.UNICODE,
            )

        m = price_regex.search(html)
        if m:
            return m.group(1).strip().replace("\xa0", " ").replace('&nbsp;', '')
        return "NaN"

    def extract_price_per_m(self, html):
        if self.country == "Slovakia":
            price_per_m2_regex = re.compile(
                r"""(?isx)
                <p[^>]*class\s*=\s*["']MuiTypography-root\s+MuiTypography-label2[^"']*["'][^>]*>  
                \s*([0-9\.\,\s,&nbsp;]+€/m²)                                                    
                """,
                re.UNICODE,
            )
        else:
            price_per_m2_regex = re.compile(
                r"""(?isx)
                <div[^>]*class\s*=\s*["']Costs-row-[^"']*["'][^>]*>    
                [^<]*?<span[^>]*>                                      
                \s*(?:Kaufpreis|Preis)(?:\s|&nbsp;|<!--.*?-->)*pro(?:\s|&nbsp;|<!--.*?-->)*m²\s* 
                </span>                                                
                .*?                                                    
                <span[^>]*class\s*=\s*["']Costs-price-[^"']*["'][^>]*> 
                \s*([0-9.,\s]+€)                                    
                \s*</span>
                """,
                re.UNICODE,
            )

        match = price_per_m2_regex.search(html)
        if match:
            return match.group(1).strip().replace("\xa0", " ").replace('&nbsp;', '')
        return "NaN"

    def detect_offer_type(self, html: str) -> str:
        country = self.country.lower()

        if country == "austria":
            offer_type_regex = re.compile(
                r"""(?isx)
                <span[^>]*class\s*=\s*["']Costs-label-[^"']*["'][^>]*>  
                \s*(Kaufpreis|Miete|Monatliche\s+Kosten)                 
                \s*</span>
                """,
                re.UNICODE,
            )
            match = offer_type_regex.search(html)
            if match:
                label = match.group(1).strip().lower()
                if "kaufpreis" in label:
                    return "buy"
                elif "miete" in label or "monatliche" in label:
                    return "rent"

        elif country == "slovakia":
            offer_type_regex = re.compile(
                r"""(?isx)
                <p[^>]*class\s*=\s*["']MuiTypography-root[^"']*["'][^>]*>
                [^<]*€
                (?:\s*/\s*(?:mes\.|mesačne|month))       
                """,
                re.UNICODE,
            )
            if offer_type_regex.search(html):
                return "rent"

            sale_regex = re.compile(
                r"""(?isx)
                <p[^>]*class\s*=\s*["']MuiTypography-root[^"']*["'][^>]*>
                [^<]*\d+[.,]?\d*\s*(?:€|EUR)(?!\s*/\s*(?:mes\.|mesačne))
                """,
                re.UNICODE,
            )
            if sale_regex.search(html):
                return "buy"

        elif country == "czech republic":
            rent_regex = re.compile(
                r"""(?isx)
                (?:měsíčně|pronájem|nájem)\b
                """,
                re.UNICODE,
            )
            if rent_regex.search(html):
                return "rent"

            buy_regex = re.compile(
                r"""(?isx)
                (?:prodej|koupě|cena)\b
                """,
                re.UNICODE,
            )
            if buy_regex.search(html):
                return "buy"

        return "NaN"

    def extract_availability(self, html):
        availability_regex = re.compile(
            r"""(?isx)
            (?:
                "availableFrom"\s*:\s*"([^"]+)" |                # JSON format
                (?:Verfügbar\s*ab|Voľný\s*od|Available\s*from)\s*[:\-]?\s*([^\n\r<\"]{1,50})  # HTML label
            )
            """,
            re.UNICODE,
        )
        match = availability_regex.search(html)
        if match:
            for g in match.groups():
                if g:
                    return g.strip()
        return "NaN"

    def extract_address(self, html):

        if self.country.lower() == "austria":
            match = re.search(r'"addressString"\s*:\s*"([^"]+)"', html)
            if match:
                return match.group(1).strip()
        else:
            address_regex = re.compile(
                r"""(?isx)
                        (?:                                                     
                            <p[^>]*class=["']MuiTypography-root[^"']*MuiTypography-noWrap[^"']*["'][^>]*>  
                            \s*([A-ZÄÖÜĽŠČŽÁÉÍÓÚÝŤĎŇa-zäöüľščžáéíóúýťďň0-9.,\- ]{10,200})\s*              
                            </p>
                        )

                        """,
                re.UNICODE
            )
            match = address_regex.search(html)
            if match:
                addr = next(g for g in match.groups() if g)
                return addr.strip().replace("\xa0", " ")
        return "NaN"
