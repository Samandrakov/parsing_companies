import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_driver():
    # Настройка драйвера Chrome
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  # Запуск в headless режиме (без открытия окна браузера)
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    logger.info("Chrome driver initialized.")
    return driver

def extract_links(driver, selector):
    # Извлечение ссылок с помощью CSS-селектора
    elements = driver.find_elements(By.CSS_SELECTOR, selector)
    links = []
    for element in elements:
        link = element.get_attribute('href')
        if link:
            links.append(link)
    logger.info(f"Extracted {len(links)} links.")
    return links

def get_next_page_url(url, page_number, page_size):
    # Пример: поиск ссылки на следующую страницу
    next_page_number = page_number + 1
    next_page_link = url.format(page_num=next_page_number, page_size=page_size)
    logger.info(f"Next page URL: {next_page_link}")
    if next_page_link:
        return next_page_link
    return None

def main(start_url, selector, current_page_number):
    driver = get_driver()
    all_links = []
    page_size = 3
    current_url = start_url.format(page_num=current_page_number, page_size=page_size)
    try:
        while current_url:
            driver.get(current_url)
            logger.info(f"Navigating to {current_url}")
            # Явное ожидание появления элементов
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector))
            )
            page_links = extract_links(driver, selector)
            all_links.extend(page_links)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        driver.quit()
        logger.info("Driver quit.")
    return all_links

if __name__ == "__main__":
    start_url = 'https://www.sgx.com/securities/corporate-information?page={page_num}&pagesize={page_size}'  # Замените на URL вашего сайта
    selector = 'a.article-list-result-item-title'
    current_page_number = 1
    links = main(start_url, selector, current_page_number)
    for link in links:
        logger.info(f"Collected link: {link}")
