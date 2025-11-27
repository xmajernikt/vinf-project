# WebDriver.py
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
class WebDriver:
    def __init__(self, chromedriver_path: str = "chromedriver", proxy: str = None, headless: bool = False):
        self.options = Options()
        if headless:
            self.options.add_argument("--headless=new")
        self.options.add_argument("--disable-gpu")
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-dev-shm-usage")


        if proxy:
            self.options.add_argument(f"--proxy-server=https://{proxy}")

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service)

    def get_driver(self):
        return self.driver

    def close(self):
        try:
            self.driver.quit()
        except Exception:
            pass


