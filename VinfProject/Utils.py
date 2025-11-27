import json
import os
import re
from datetime import datetime

from selenium.common import JavascriptException, TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait

from models.url_record import UrlRecord
import time

class Utils:
    def __init__(self, metadata_path):
        self.metadata_path = metadata_path

    def check_if_link_exists(self, url_record: UrlRecord, country: str) -> bool:
        metadata_path_file = os.path.join(self.metadata_path, f"{country}_metadata.json")

        try:
            with open(metadata_path_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                return any(entry["property_url"] == url_record.property_url for entry in data)
        except (json.JSONDecodeError, FileNotFoundError):
            return False

    def save_link(self, url_record: UrlRecord, country: str):
        metadata_path_file = os.path.join(self.metadata_path, f"{country}_metadata.json")


        if not os.path.exists(metadata_path_file):
            with open(metadata_path_file, "w", encoding="utf-8") as f:
                json.dump([], f, ensure_ascii=False, indent=2)

        if self.check_if_link_exists(url_record, country):
            print(f"Skipping duplicate: {url_record.property_url}")
            return

        try:
            with open(metadata_path_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            data = []

        data.append(url_record.to_dict())

        with open(metadata_path_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print(f"Added new URL: {url_record.property_url}")


    @staticmethod
    def sanitize_filename(name: str) -> str:
        return re.sub(r'[\\/*?:"<>|]', "_", name)

    @staticmethod
    def save_page(html: str, pages_dir: str, url: str = None) -> str:
        os.makedirs(pages_dir, exist_ok=True)
        if url:
            base_name = Utils.sanitize_filename(url)
        else:
            base_name = datetime.now().strftime("%Y%m%d_%H%M%S")

        file_path = os.path.join(pages_dir, f"{base_name}.html")

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(html)

        print(f"[INFO] Saved page to {file_path}")
        return file_path

    @staticmethod
    def find_last_page_index(html: str) -> int:
        pattern = r'(?:[?&](?:page|strana)=|/(?:seite|strana|page)[=-])(\d{1,5})'
        matches = re.findall(pattern, html, flags=re.IGNORECASE)
        if not matches:
            return 1
        return max(map(int, matches))

    def check_if_visited(self, property_url: str, country: str) -> bool:
        try:
            metadata_path_file = os.path.join(self.metadata_path, f"{country}_metadata.json")

            with open(metadata_path_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                return any(entry["property_url"] == property_url and entry["saved"] is False
                           for entry in data)
        except (json.JSONDecodeError, FileNotFoundError):
            return False

    @staticmethod
    def scroll_to_load_all(driver, pause_time=0.3, scroll_step=1000):

        print("Starting fixed-step scrolling...")

        total_height = driver.execute_script("return document.body.scrollHeight")
        current_position = 0
        iteration = 0

        while current_position < total_height:
            driver.execute_script(f"window.scrollTo(0, {current_position});")
            time.sleep(pause_time)

            current_position += scroll_step
            iteration += 1

            if iteration % 10 == 0:
                total_height = driver.execute_script("return document.body.scrollHeight")

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)

        total_links = driver.execute_script("return document.body.innerHTML").count("/expose/")
        print(f"Scrolling complete after {iteration} iterations.")
        print(f"Found {total_links} property links (via '/expose/').")

        return total_links

    @staticmethod
    def accept_usercentrics_cookies(driver, max_wait=20):

        print("Waiting for Usercentrics cookie banner...")
        start_time = time.time()

        while time.time() - start_time < max_wait:
            try:
                button = driver.execute_script("""
                    const root = document.querySelector('#usercentrics-root');
                    if (!root || !root.shadowRoot) return null;
                    return root.shadowRoot.querySelector('button[data-testid="uc-accept-all-button"]');
                """)
                if button:
                    driver.execute_script("arguments[0].scrollIntoView(true);", button)
                    time.sleep(0.5)
                    driver.execute_script("arguments[0].click();", button)
                    print("Usercentrics cookies accepted.")
                    return True
            except JavascriptException:
                pass

            driver.execute_script("window.scrollBy(0, 300);")
            time.sleep(0.5)

        print("Could not find the Usercentrics accept button.")
        return False

    @staticmethod
    def accept_any_cookies(driver, max_wait=25):
        print("Waiting for cookie banner (Usercentrics or ConsentWall)...")
        start_time = time.time()

        while time.time() - start_time < max_wait:
            try:
                usercentrics_btn = driver.execute_script("""
                    const root = document.querySelector('#usercentrics-root');
                    if (!root || !root.shadowRoot) return null;
                    return root.shadowRoot.querySelector('button[data-testid="uc-accept-all-button"]');
                """)
                if usercentrics_btn:
                    driver.execute_script("arguments[0].scrollIntoView({behavior:'smooth', block:'center'});", usercentrics_btn)
                    time.sleep(0.8)
                    driver.execute_script("""
                        arguments[0].click();
                        arguments[0].dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
                    """, usercentrics_btn)
                    print("Usercentrics cookies accepted.")
                    break

                consentwall_btn = driver.execute_script("""
                    const container = document.querySelector('.szn-cmp-dialog-container');
                    if (!container || !container.shadowRoot) return null;
                    return container.shadowRoot.querySelector('button[data-testid="cw-button-agree-with-ads"]');
                """)
                if consentwall_btn:
                    driver.execute_script("arguments[0].scrollIntoView({behavior:'smooth', block:'center'});", consentwall_btn)
                    time.sleep(0.8)
                    driver.execute_script("""
                        arguments[0].click();
                        arguments[0].dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
                    """, consentwall_btn)
                    print("ConsentWall cookies accepted.")
                    break

            except JavascriptException:
                pass

            driver.execute_script("window.scrollBy(0, 200);")
            time.sleep(0.5)

        try:
            WebDriverWait(driver, 8).until_not(
                lambda d: d.execute_script("return document.querySelector('.szn-cmp-dialog-container') !== null;")
            )
            print("Banner fully disappeared.")
            return True
        except TimeoutException:
            print("Banner did not disappear completely, continuing anyway.")
            return False

    @staticmethod
    def is_interesting_page(html: str, pattern_validation) -> bool:
        return re.search(pattern_validation, html, re.IGNORECASE) is not None

    @staticmethod
    def detect_pagination_param(html: str) -> str:

        match = re.search(r"[?&](page|seite|strana)=", html, re.IGNORECASE)
        if match:
            return match.group(1)
        return "page"
