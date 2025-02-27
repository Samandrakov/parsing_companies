import json
import logging

logger = logging.getLogger(__name__)

def parsing_data(self, links, output_file, save_every=None):
    driver = self.driver_manager.driver
    all_data = []

    for index, link in enumerate(links):
        driver.get(link)
        logger.info(f'Started parsing {link}')
        terms = driver.find_elements(By.CSS_SELECTOR, 'dl dt')
        definitions = driver.find_elements(By.CSS_SELECTOR, 'dl dd')
        data = {}

        for term, definition in zip(terms, definitions):
            term_text = term.text.strip(' :')
            definition_text = definition.text.strip()
            data[term_text] = definition_text

        all_data.append(data)
        logger.info(f"Extracted {len(data)} items.")

        # Сохранение данных в файл через каждые `save_every` итерации
        if save_every and (index + 1) % save_every == 0:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(all_data, f, ensure_ascii=False, indent=4)
            logger.info(f"Data saved to {output_file} after {index + 1} iterations.")

    # Финальное сохранение данных после завершения цикла
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=4)
    logger.info(f"Final data saved to {output_file}.")

    return all_data
