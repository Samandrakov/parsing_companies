import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
class DriverManager:
    def __init__(self):
        self.driver = None

    def start_driver(self):
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)
        logger.info("Chrome driver initialized.")

    def quit_driver(self):
        if self.driver:
            self.driver.quit()
            logger.info("Driver quit.")