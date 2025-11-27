import os
import random
import re
import time
import json
import logging
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse
from queue import Queue
from Utils import Utils
from models.scrape_params import ScrapeParam
from VinfProject.models.url_record import UrlRecord
from VinfProject.models.last_scrape_params import LastScrapeParams

class Crawler:
    def __init__(self, pages_path, metadata_path: str, driver=None, log_dir="logs"):
        self.pages_path = pages_path
        self.driver = driver
        self.utils = Utils(metadata_path)
        self.metadata_path = metadata_path
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        ]
        self.headers = {
            "User-Agent": random.choice(self.user_agents),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }

        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, "crawler.log")

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
            handlers=[
                logging.FileHandler(log_path, encoding="utf-8"),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger("Crawler")
        self.logger.info("Crawler initialized.")


    def save_page(self, property_url: str, country: str, html, pages_path) -> str:
        page_path = ""
        try:
            if not self.utils.check_if_visited(property_url, country):
                self.logger.debug(f"Visiting {property_url}")
                page_path = Utils.save_page(html, pages_path, property_url)
                self.logger.info(f"Saved page for {property_url}")
        except Exception as e:
            self.logger.exception(f"Error saving page for {property_url}: {e}")

        return page_path

    def scrape_urls(self, scrape_params: ScrapeParam, visited=None, queue=None, downloaded_count=0):
        if visited is None:
            visited = set()

        pages_dir = os.path.join(self.pages_path, scrape_params.country)
        if queue is None:
            queue = Queue()
            start_url = scrape_params.root_url
            queue.put(start_url)
        else:
            start_url = queue.get()
        base_url = start_url
        parsed = urlparse(scrape_params.root_url)
        if scrape_params.country != "Slovakia":
            base_url = f"{parsed.scheme}://{parsed.netloc}"

        self.logger.info(f"Starting URL scraping from: {start_url}")
        all_links = set()
        current_page = 1
        html_path = None
        state_file = "last_scrape_params.json"

        try:
            self.driver.get(start_url)
            time.sleep(random.uniform(5, 7.5))

            Utils.accept_any_cookies(self.driver)

        except Exception as e:
            self.logger.exception(f"Error navigating to start URL {start_url}: {e}")

        try:
            while not queue.empty() and downloaded_count < scrape_params.max_saves:
                saved = False

                current_url = queue.get()
                if current_url in visited:
                    continue
                visited.add(current_url)

                try:
                    self.logger.info(f"Visiting: {current_url}")
                    self.driver.get(current_url)
                    if scrape_params.country == "Austria":
                        Utils.scroll_to_load_all(self.driver)

                    time.sleep(random.uniform(5, 7.5))

                    html = self.driver.page_source

                    if current_page == 1:
                        max_pages = self.utils.find_last_page_index(html)
                        self.logger.info(f"Detected {max_pages} total pages.")

                    if re.match(rf"{scrape_params.property_url_regex_full}", urljoin(base_url, current_url)):
                        if scrape_params.validation_pattern and Utils.is_interesting_page(html,
                                                                                          scrape_params.validation_pattern):
                            html_path = self.save_page(current_url, scrape_params.country, html, pages_dir)
                            downloaded_count += 1
                            self.logger.info(f"Saved page: {current_url}")
                            saved = True
                    url_record = UrlRecord(
                        property_url=current_url,
                        saved=saved,
                        date_saved=str(datetime.now()),
                        date_visited=str(datetime.now()),
                        html_path=html_path,
                        country=scrape_params.country,
                    )
                    self.utils.save_link(url_record, scrape_params.country)

                    links = {
                        urljoin(base_url, link)
                        for link in re.findall(rf"{scrape_params.property_url_regex}", html)
                    }

                    new_links = links - visited
                    all_links |= new_links

                    self.logger.info(f"Found {len(new_links)} new links on page.")

                    _ = list(map(queue.put, new_links))

                except Exception as e:
                    self.logger.exception(f"Error processing {current_url}: {e}")
                    continue

        except KeyboardInterrupt:
            self.logger.warning("Scraping interrupted manually. Saving current state...")
            self.save_scrape_state(scrape_params, downloaded_count, visited, queue, state_file)
            raise

        except Exception as e:
            self.logger.exception(f"Unexpected error: {e}. Saving state before exit.")
            self.save_scrape_state(scrape_params, downloaded_count, visited, queue, state_file)
            raise


        self.logger.info(f"Scraping finished. Total unique links collected: {len(all_links)}")

    def save_scrape_state(self, scrape_params, downloaded_count, visited, queue, path):
        state_dict = {
            "scrape_param": scrape_params.__dict__,
            "downloaded_count": downloaded_count,
            "visited": list(visited),  # sets -> lists for JSON
            "queue_items": list(queue.queue)  # queue -> list for JSON
        }

        path = Path(path)
        tmp_path = path.with_suffix(".tmp")

        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(state_dict, f, ensure_ascii=False, indent=2)

        os.replace(tmp_path, path)
        self.logger.info(f"Saved scraper state to {path}")

