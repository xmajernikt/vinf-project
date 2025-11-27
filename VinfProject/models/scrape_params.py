from dataclasses import dataclass

@dataclass
class ScrapeParam:
    root_url: str = ""
    country: str = ""
    property_url_regex: str = ""
    validation_pattern: str = ""
    page_naming:str = ""
    property_url_regex_full:str = ""
    max_saves: int = 500