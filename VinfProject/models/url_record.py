from dataclasses import dataclass

@dataclass
class UrlRecord:
    def __init__(self, property_url: str, saved: bool, date_saved: str, date_visited: None | str, html_path: str | None,
                 country):
        self.property_url: str = property_url
        self.saved: bool = saved
        self.date_saved: str = date_saved
        self.date_visited: None | str = date_visited
        self.html_path: None | str = html_path
        self.country: str | None = country


    def to_dict(self):
        return {
            "property_url": self.property_url,
            "saved": self.saved,
            "date_saved": self.date_saved,
            "date_visited": self.date_visited,
            "html_path": self.html_path,
            "country": self.country
        }