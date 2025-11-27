# main.py (or your existing main, replace relevant parts)

import os
import json

from WebDriver import WebDriver
from Crawler import Crawler
from proxy_manager import ProxyManager
from models.scrape_params import ScrapeParam
from models.last_scrape_params import LastScrapeParams
from indexer import Indexer
# GLOBALS
RETRY_SCRAPE = False


def main():
    base_url = "https://www.nehnutelnosti.sk"
    save_dir_metadata = "metadata"
    config_path = "scrape_config.json"
    scrape_param_conf = "scrape_params.json"
    pages_path = "pages"
    saved_scrape_data = list()
    last_scrape_params_json = "last_scrape_params.json"
    resume = os.path.exists(last_scrape_params_json)
    slice_index = 0


    with open(scrape_param_conf, "r", encoding="utf-8") as f:
        scrape_params = json.load(f)


    params = [ScrapeParam(**item) for item in scrape_params["scrape_params"]]
    if resume:
        print("Resuming previous scraping session...")
        with open(last_scrape_params_json, "r", encoding="utf-8") as f:
            saved_state_data = json.load(f)
            saved_state = LastScrapeParams.from_dict(saved_state_data)


    if not os.path.exists(save_dir_metadata):
        os.makedirs(save_dir_metadata)

    addon_path = os.path.abspath("modify_headers.py")
    proxy_port = 8080
    pm = ProxyManager(addon_path=addon_path, port=proxy_port)
    print("Starting mitmproxy...")
    pm.start()
    print("mitmproxy started on port", proxy_port)

    proxy_addr = f"127.0.0.1:{proxy_port}"
    chromedriver_path = "chromedriver"
    driver_manager = WebDriver(chromedriver_path=chromedriver_path, proxy=proxy_addr, headless=False)
    driver = driver_manager.get_driver()
    crawler = Crawler(pages_path, save_dir_metadata, driver)

    if resume:
        for i, param in enumerate(params):
            if param.root_url == saved_state.scrape_param.root_url:
                slice_index = i
                break
    try:
        if not resume:
            for param in params:
                crawler.scrape_urls(param)

        else:
            for param in params[slice_index:]:
                crawler.scrape_urls(param, saved_state.visited, saved_state.queue_items, saved_state.downloaded_count)
    finally:
        print("Tearing down...")
        driver_manager.close()
        pm.stop()
        print("Shutdown complete.")

if __name__ == "__main__":
    # main()
    indexer = Indexer()
    indexer.load_from_tsv("properties.tsv")
    indexer.build_index()
    indexer.save_index("index_data.json")

    indexer.show_results("trnava house buy energy_class A", idf_type="classic")
    indexer.show_results("trnava house buy energy_class A", idf_type="smooth")
    indexer.show_results("trnava house buy energy_class A", idf_type="probabilistic")
