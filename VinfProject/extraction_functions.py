import re

def detect_country(html: str) -> str:
    html_lower = html.lower()
    if "nehnutelnosti.sk" in html_lower:
        return "Slovakia"
    elif "sreality.cz" in html_lower:
        return "Czechia"
    elif "immobilienscout24.at" in html_lower or "immowelt.at" in html_lower or "willhaben.at" in html_lower:
        return "Austria"
    return "Unknown"


def detect_property_type(html: str) -> str:
    html_lower = html.lower()
    if re.search(r"\b(byt|wohnung|flat|apartment|apartmán)\b", html_lower):
        return "flat"
    elif re.search(r"\b(dom|haus|family house|dům|rodinný dům|rodinný dom)\b", html_lower):
        return "house"
    return "unknown"


def detect_offer_type(html: str, country) -> str:

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
            <p[^>]*class\s*=\s*["']MuiTypography-root\s+MuiTypography-h3\s+mui-15163pd[^"']*["'][^>]*>
            [^<]*€
            (?:&nbsp;|\s|<!--.*?-->)*
            \s*/\s*(?:mes\.|mesačne|month)
            """,
            re.UNICODE,
        )
        if offer_type_regex.search(html):
            return "rent"

        sale_regex = re.compile(
            r"""(?isx)
            <p[^>]*class\s*=\s*["']MuiTypography-root\s+MuiTypography-h3\s+mui-15163pd[^"']*["'][^>]*>
            [^<]*\d+[.,]?\d*\s*(?:€|EUR)(?!\s*/\s*(?:mes\.|mesačne))
            """,
            re.UNICODE,
        )
        if sale_regex.search(html):
            return "buy"

    return "NaN"


def extract_field(html, pattern):
    m = re.search(pattern, html, re.IGNORECASE)
    return m.group(1).strip() if m else "NaN"


def extract_address(html, country):
    if country.lower() == "austria":
        m = re.search(r'"addressString"\s*:\s*"([^"]+)"', html)
        if m:
            return m.group(1).strip()

    m = re.search(
        r"""(?isx)
        <p[^>]*class=["']MuiTypography-root[^"']*MuiTypography-noWrap[^"']*["'][^>]*>
        \s*([A-ZÄÖÜĽŠČŽÁÉÍÓÚÝŤĎŇa-zäöüľščžáéíóúýťďň0-9.,\- ]{10,200})\s*
        </p>""",
        html
    )
    if m:
        return m.group(1).strip().replace("\xa0", " ")

    return "NaN"


def extract_energy_class(html):
    m = re.search(
        r"""(?isx)
        <div[^>]*class\s*=\s*["']Indicator-indicator-aA6["'][^>]*>
        .*?
        <span[^>]*>\s*(A\+\+|A\+|A0|A1|A2|A|B|C|D|E|F|G)\s*</span>""",
        html
    )
    if m:
        return m.group(1)

    m2 = re.search(r"\b(A\+\+|A\+|A0|A1|A2|A|B|C|D|E|F|G)\b", html)
    return m2.group(1) if m2 else "NaN"


def extract_price(html, country):
    if country == "Slovakia":
        pattern = r"<p[^>]*MuiTypography-h3[^>]*>\s*([0-9\.\s,&nbsp;]+€)"
    else:
        pattern = r"""(?isx)
        <span[^>]*class\s*=\s*["']Costs-label-[^"']*["'][^>]*>
        \s*(?:Kaufpreis|Miete|Monatliche\s+Kosten)\s*</span>
        .*?
        <span[^>]*class\s*=\s*["']Costs-price-[^"']*["'][^>]*>
        \s*([0-9.,\s]+€)\s*</span>"""

    m = re.search(pattern, html, re.IGNORECASE)
    return m.group(1).replace("\xa0", " ").replace("\xa0", " ").replace('&nbsp;', '') if m else "NaN"


def extract_price_per_m2(html, country):
    if country == "Slovakia":
        pattern = r"<p[^>]*MuiTypography-label2[^>]*>\s*([0-9\.,\s]+€/m²)"
    else:
        pattern = r"""(?isx)
        <span[^>]*>\s*(?:Kaufpreis|Preis).*?pro.*?m²\s*</span>
        .*?
        <span[^>]*Costs-price-[^"']*[^>]*>\s*([0-9.,\s]+€)\s*</span>"""

    m = re.search(pattern, html)
    return m.group(1).replace("\xa0", " ").replace("\xa0", " ").replace('&nbsp;', '') if m else "NaN"


def extract_availability(html):
    m = re.search(
        r"""(?isx)
        "availableFrom"\s*:\s*"([^"]+)" |
        (?:Verfügbar\s*ab|Voľný\s*od|Available\s*from)\s*[:\-]?\s*([^\n\r<"]{1,50})
        """,
        html
    )
    if m:
        return next(g for g in m.groups() if g)
    return "NaN"

def extract_city(address, country: str):
    address_components = address.split(",")
    city = "NaN"
    if country.lower() == "slovakia":

        if len(address_components) == 3:
            city = address_components[1]
        else:
            city = address_components[0]
    else:
        if len(address_components) == 3:
            city = address_components[2]
        elif len(address_components) == 2:
            city = address_components[1]
        elif len(address_components) == 1:
            city = address_components[0]

        city = re.search(r"[^\d].*", city).group(0)
    return city.replace("\xa0", " ").replace("\xa0", " ").replace('&nbsp;', '')