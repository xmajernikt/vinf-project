from dataclasses import dataclass, asdict
from VinfProject.models.scrape_params import ScrapeParam
from queue import Queue
from typing import List, Set


@dataclass
class LastScrapeParams:
    scrape_param: ScrapeParam
    downloaded_count: int
    visited: Set[str]
    queue_items: Queue

    def __init__(self, scrape_param: ScrapeParam, downloaded_count: int = 0, visited=None, queue_items=None):
        self.scrape_param = scrape_param
        self.downloaded_count = downloaded_count
        self.visited = set(visited) if visited else set()
        self.queue_items = queue_items if isinstance(queue_items, Queue) else Queue()
        if isinstance(queue_items, list):  # rebuild queue if list passed
            for item in queue_items:
                self.queue_items.put(item)

    def to_dict(self):
        """Convert the state into a JSON-serializable dictionary."""
        return {
            "scrape_param": asdict(self.scrape_param),
            "downloaded_count": self.downloaded_count,
            "visited": list(self.visited),  # convert set to list
            "queue_items": list(self.queue_items.queue),  # extract list from Queue
        }

    @classmethod
    def from_dict(cls, d):
        """Recreate the object from a dictionary."""
        scrape_param = ScrapeParam(**d["scrape_param"])
        visited = set(d.get("visited", []))
        queue_items = Queue()
        for item in d.get("queue_items", []):
            queue_items.put(item)
        return cls(scrape_param, d["downloaded_count"], visited, queue_items)
