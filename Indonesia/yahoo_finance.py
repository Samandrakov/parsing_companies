# company_enricher_improved.py
import json
import pandas as pd
import yfinance as yf
import time
import logging
from typing import Dict, Any, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ImprovedCompanyDataEnricher:
    def __init__(self, input_file: str, mapping_file: str = None):
        self.input_file = input_file
        self.companies_data = self.load_companies_data()
        self.ticker_mapping = self.load_ticker_mapping(mapping_file)

    def load_companies_data(self) -> list:
        """Загружает исходные данные компаний из JSON файла"""
        try:
            with open(self.input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logger.info(f"Успешно загружено {len(data)} компаний из {self.input_file}")
            return data
        except Exception as e:
            logger.error(f"Ошибка загрузки файла {self.input_file}: {e}")
            return []

    def load_ticker_mapping(self, mapping_file: str) -> Dict[str, str]:
        """Загружает mapping тикеров из файла или создает базовый"""
        if mapping_file:
            try:
                with open(mapping_file, 'r', encoding='utf-8') as f:
                    mapping = json.load(f)
                logger.info(f"Загружено {len(mapping)} тикеров из {mapping_file}")
                return mapping
            except:
                logger.warning(f"Не удалось загрузить {mapping_file}, используем базовый mapping")

        # Базовый mapping для самых известных компаний
        return {
            "PT Adaro Andalan Indonesia Tbk": "ADRO.JK",
            "Astra Agro Lestari Tbk": "AALI.JK",
            "PT Bank Central Asia Tbk": "BBCA.JK",
            "PT Bank Mandiri (Persero) Tbk": "BMRI.JK",
            "PT Bank Rakyat Indonesia (Persero) Tbk": "BBRI.JK",
            "PT Telekomunikasi Indonesia Tbk": "TLKM.JK",
            "PT Unilever Indonesia Tbk": "UNVR.JK",
        }

    def smart_ticker_lookup(self, company_name: str) -> Optional[str]:
        """Умный поиск тикера с несколькими стратегиями"""
        # Прямой поиск в mapping
        if company_name in self.ticker_mapping:
            return self.ticker_mapping[company_name]

        # Попробуем найти по ключевым словам
        name_lower = company_name.lower()

        # Эвристики для поиска тикеров
        heuristics = [
            # Банки
            ('bank central asia', 'BBCA.JK'),
            ('bank mandiri', 'BMRI.JK'),
            ('bank rakyat', 'BBRI.JK'),
            ('bank bca', 'BBCA.JK'),
            ('bri', 'BBRI.JK'),

            # Телеком
            ('telekomunikasi', 'TLKM.JK'),
            ('telkom', 'TLKM.JK'),
            ('indosat', 'ISAT.JK'),

            # Потребительские товары
            ('unilever', 'UNVR.JK'),
            ('kalbe farma', 'KLBF.JK'),
            ('indofood', 'INDF.JK'),

            # Энергия
            ('adaro', 'ADRO.JK'),
            ('bukit asam', 'PTBA.JK'),
            ('medco energi', 'MEDC.JK'),

            # Плантации
            ('astra agro', 'AALI.JK'),
            ('pp london sumatra', 'LSIP.JK'),
            ('sampoerna agro', 'SGRO.JK'),
        ]

        for keyword, ticker in heuristics:
            if keyword in name_lower:
                logger.info(f"Найден тикер по ключевому слову: {company_name} -> {ticker}")
                return ticker

        return None

    def enrich_company_data(self, company_data: Dict[str, Any]) -> Dict[str, Any]:
        """Обогащает данные компании финансовыми показателями"""
        company_name = company_data.get("Full Company Name", "")
        ticker = self.smart_ticker_lookup(company_name)

        if not ticker:
            company_data["Enriched_Financial_Data"] = {
                "Ticker": None,
                "Data_Retrieval_Status": "Ticker not found",
                "Last_Updated": pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            return company_data

        try:
            logger.info(f"Получение данных для {company_name} ({ticker})")

            company = yf.Ticker(ticker)
            info = company.info

            # Базовые финансовые показатели
            enriched_data = {
                "Ticker": ticker,
                "Current_Price": info.get('currentPrice'),
                "Market_Cap": info.get('marketCap'),
                "Currency": info.get('currency'),
                "PE_Ratio": info.get('trailingPE'),
                "PB_Ratio": info.get('priceToBook'),
                "ROE": info.get('returnOnEquity'),
                "Revenue": info.get('totalRevenue'),
                "Net_Income": info.get('netIncomeToCommon'),
                "Dividend_Yield": info.get('dividendYield'),
                "Employees": info.get('fullTimeEmployees'),
                "Sector_YF": info.get('sector'),
                "Industry_YF": info.get('industry'),
                "Data_Retrieval_Status": "Success",
                "Last_Updated": pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            company_data["Enriched_Financial_Data"] = enriched_data
            logger.info(f"Успешно обогащены данные для {company_name}")

        except Exception as e:
            logger.error(f"Ошибка получения данных для {company_name} ({ticker}): {e}")
            company_data["Enriched_Financial_Data"] = {
                "Ticker": ticker,
                "Data_Retrieval_Status": f"Error: {str(e)}",
                "Last_Updated": pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            }

        time.sleep(0.5)  # Задержка между запросами
        return company_data

    def enrich_all_companies(self, batch_size: int = None) -> list:
        """Обогащает данные всех компаний"""
        enriched_data = []
        total_companies = len(self.companies_data)

        if batch_size:
            companies_to_process = self.companies_data[:batch_size]
        else:
            companies_to_process = self.companies_data

        logger.info(f"Начало обогащения данных для {len(companies_to_process)} компаний...")

        for i, company in enumerate(companies_to_process, 1):
            logger.info(f"Обработка компании {i}/{len(companies_to_process)}")
            enriched_company = self.enrich_company_data(company)
            enriched_data.append(enriched_company)

        logger.info("Обогащение данных завершено!")
        return enriched_data

    def save_enriched_data(self, enriched_data: list, json_output: str, csv_output: str):
        """Сохраняет обогащенные данные в JSON и CSV форматах"""

        # Сохранение в JSON
        try:
            with open(json_output, 'w', encoding='utf-8') as f:
                json.dump(enriched_data, f, ensure_ascii=False, indent=2)
            logger.info(f"Данные сохранены в JSON: {json_output}")
        except Exception as e:
            logger.error(f"Ошибка сохранения JSON: {e}")

        # Сохранение в CSV
        try:
            flat_data = []
            for company in enriched_data:
                flat_company = company.copy()
                financial_data = flat_company.pop('Enriched_Financial_Data', {})
                for key, value in financial_data.items():
                    flat_company[f'Financial_{key}'] = value
                flat_data.append(flat_company)

            df = pd.DataFrame(flat_data)
            df.to_csv(csv_output, index=False, encoding='utf-8')
            logger.info(f"Данные сохранены в CSV: {csv_output}")

        except Exception as e:
            logger.error(f"Ошибка сохранения CSV: {e}")


def main():
    # Конфигурация
    INPUT_FILE = "idx_stocks.json"
    MAPPING_FILE = "ticker_mapping.json"  # Сначала создайте этот файл
    JSON_OUTPUT = "idx_stocks_enriched.json"
    CSV_OUTPUT = "idx_stocks_enriched.csv"

    # Создаем обогатитель данных
    enricher = ImprovedCompanyDataEnricher(INPUT_FILE, MAPPING_FILE)

    # Проверяем, что данные загружены
    if not enricher.companies_data:
        logger.error("Не удалось загрузить данные компаний. Завершение работы.")
        return

    # Обогащаем данные (можно указать batch_size для тестирования)
    enriched_data = enricher.enrich_all_companies(batch_size=50)  # Обработать первые 50 для теста

    # Сохраняем результаты
    enricher.save_enriched_data(enriched_data, JSON_OUTPUT, CSV_OUTPUT)

    # Статистика
    successful_updates = sum(1 for company in enriched_data
                             if company.get('Enriched_Financial_Data', {}).get('Data_Retrieval_Status') == 'Success')

    print(f"\n=== ОТЧЕТ ===")
    print(f"Всего обработано: {len(enriched_data)}")
    print(f"Успешно обновлено: {successful_updates}")
    print(f"Не удалось обновить: {len(enriched_data) - successful_updates}")
    print(f"JSON файл: {JSON_OUTPUT}")
    print(f"CSV файл: {CSV_OUTPUT}")


if __name__ == "__main__":
    main()