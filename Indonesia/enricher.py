import pandas as pd
import json
from typing import Dict, List, Any, Optional


def load_and_merge_data(csv_file: str, json_file: str, output_file: str):
    """
    Основная функция для объединения данных из CSV и JSON в новый JSON
    """
    print("=== ОБЪЕДИНЕНИЕ CSV И JSON ДАННЫХ ===")

    # Загружаем CSV данные
    print("1. Загрузка CSV данных...")
    try:
        df_csv = pd.read_csv(csv_file)
        print(f"   Загружено {len(df_csv)} компаний из CSV")
    except Exception as e:
        print(f"   Ошибка загрузки CSV: {e}")
        return

    # Загружаем JSON данные
    print("2. Загрузка JSON данных...")
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        print(f"   Загружено {len(json_data)} компаний из JSON")
    except Exception as e:
        print(f"   Ошибка загрузки JSON: {e}")
        return

    # Создаем словарь для быстрого поиска в JSON данных
    print("3. Создание индекса для поиска...")
    json_by_ticker = create_json_index(json_data)
    json_by_name = create_name_index(json_data)

    # Объединяем данные
    print("4. Объединение данных...")
    merged_data = merge_datasets(df_csv, json_by_ticker, json_by_name)

    # Сохраняем результат
    print("5. Сохранение результата...")
    save_merged_data(merged_data, output_file)

    print(f"\n=== РЕЗУЛЬТАТ ===")
    print(f"Объединенный файл сохранен: {output_file}")
    print(f"Всего компаний в результате: {len(merged_data)}")


def create_json_index(json_data: List[Dict]) -> Dict[str, Dict]:
    """
    Создает индекс для поиска по тикеру из JSON данных
    """
    index = {}
    for company in json_data:
        # Пробуем разные возможные поля с тикером
        possible_ticker_fields = ['ISIN Code', 'Ticker', 'Code', 'Symbol']
        for field in possible_ticker_fields:
            ticker = company.get(field)
            if ticker and isinstance(ticker, str) and ticker.strip():
                index[ticker.strip().upper()] = company
                break
    return index


def create_name_index(json_data: List[Dict]) -> Dict[str, Dict]:
    """
    Создает индекс для поиска по названию компании
    """
    index = {}
    for company in json_data:
        name = company.get('Full Company Name', '')
        if name:
            clean_name = clean_company_name(name)
            index[clean_name] = company
    return index


def clean_company_name(name: str) -> str:
    """
    Очищает название компании для сравнения
    """
    if not isinstance(name, str):
        return ""
    return (name.lower()
            .replace('pt ', '')
            .replace(' tbk', '')
            .replace('.', '')
            .replace(',', '')
            .replace('persero', '')
            .strip())


def find_matching_json_company(csv_row: pd.Series, json_by_ticker: Dict, json_by_name: Dict) -> Optional[Dict]:
    """
    Находит соответствующую компанию в JSON данных
    """
    # Пытаемся найти по тикеру из CSV
    csv_ticker = str(csv_row.get('Ticker', '')).strip().upper()
    if csv_ticker in json_by_ticker:
        return json_by_ticker[csv_ticker]

    # Пытаемся найти по названию компании из CSV
    csv_company_name = csv_row.get('Company_name', '')
    if csv_company_name:
        clean_csv_name = clean_company_name(csv_company_name)

        # Прямое сравнение
        if clean_csv_name in json_by_name:
            return json_by_name[clean_csv_name]

        # Поиск по частичному совпадению
        for json_name, json_company in json_by_name.items():
            if clean_csv_name in json_name or json_name in clean_csv_name:
                return json_company

    return None


def merge_company_data(csv_row: pd.Series, json_company: Optional[Dict]) -> Dict[str, Any]:
    """
    Объединяет данные из CSV и JSON для одной компании
    """
    merged_company = {}

    # Добавляем данные из CSV (финансовые показатели)
    csv_data = {
        # Основная информация
        'Ticker': csv_row.get('Ticker'),
        'Company_name': csv_row.get('Company_name'),

        # Финансовые метрики
        'Market_Cap': csv_row.get('Market_Cap'),
        'Stock_price': csv_row.get('Stock_price'),
        'Volume': csv_row.get('Volume'),

        # Отраслевая информация из CSV
        'Industry_CSV': csv_row.get('Industry'),
        'Sector_CSV': csv_row.get('Sector'),

        # Финансовые результаты
        'Revenue': csv_row.get('Revenue'),
        'Revenue_Growth': csv_row.get('Rev. Growth'),
        'Net_Income': csv_row.get('Net_Income'),
        'Free_Cash_Flow': csv_row.get('FCF')
    }

    # Очищаем от NaN значений
    for key, value in csv_data.items():
        if pd.notna(value):
            merged_company[key] = value

    # Добавляем данные из JSON, если они есть
    if json_company:
        json_fields = {
            'Full_Company_Name': 'Full Company Name',
            'ISIN_Code': 'ISIN Code',
            'Registered_Office': 'Registered Office',
            'Telephone': 'Telephone',
            'Fax': 'Fax',
            'Email': 'Email',
            'Taxpayer_Number': 'Taxpayer Number',
            'Website': 'Link to Internet Website',
            'Main_Business_Fields': 'Main Business Fields',
            'Sector_JSON': 'Sector',
            'Subsector': 'Subsector',
            'Industry_JSON': 'Industry',
            'Sub_Industry': 'Sub-industry',
            'Securities_Administration_Bureau': 'Securities Administration Bureau',
            'Recording_Date': 'Recording Date',
            'Recording_Board': 'Recording Board'
        }

        for new_key, json_key in json_fields.items():
            value = json_company.get(json_key)
            if value and str(value).strip():
                merged_company[new_key] = value

    # Добавляем флаг успешного объединения
    merged_company['Data_Merge_Success'] = json_company is not None
    merged_company['Data_Source'] = 'CSV+JSON' if json_company else 'CSV_only'

    return merged_company


def merge_datasets(df_csv: pd.DataFrame, json_by_ticker: Dict, json_by_name: Dict) -> List[Dict]:
    """
    Объединяет данные из CSV и JSON
    """
    merged_data = []
    match_count = 0

    for index, row in df_csv.iterrows():
        json_company = find_matching_json_company(row, json_by_ticker, json_by_name)
        merged_company = merge_company_data(row, json_company)
        merged_data.append(merged_company)

        if json_company:
            match_count += 1

        # Прогресс
        if (index + 1) % 50 == 0:
            print(f"   Обработано {index + 1}/{len(df_csv)} компаний")

    print(f"   Найдено совпадений: {match_count}/{len(df_csv)}")

    return merged_data


def save_merged_data(merged_data: List[Dict], output_file: str):
    """
    Сохраняет объединенные данные в JSON файл
    """
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(merged_data, f, ensure_ascii=False, indent=2)


def generate_statistics(merged_data: List[Dict]):
    """
    Генерирует статистику по объединенным данным
    """
    total_companies = len(merged_data)
    merged_companies = sum(1 for company in merged_data if company.get('Data_Merge_Success'))
    csv_only_companies = total_companies - merged_companies

    print(f"\n=== СТАТИСТИКА ОБЪЕДИНЕНИЯ ===")
    print(f"Всего компаний: {total_companies}")
    print(f"Успешно объединено: {merged_companies} ({merged_companies / total_companies * 100:.1f}%)")
    print(f"Только из CSV: {csv_only_companies} ({csv_only_companies / total_companies * 100:.1f}%)")

    # Статистика по наличию данных
    fields_to_check = ['Revenue', 'Market_Cap', 'Registered_Office', 'Website', 'Email']
    print(f"\nНаличие данных по полям:")
    for field in fields_to_check:
        count = sum(1 for company in merged_data if company.get(field))
        print(f"  {field}: {count}/{total_companies} ({count / total_companies * 100:.1f}%)")


# Упрощенная версия для быстрого использования
def quick_merge(csv_file: str, json_file: str, output_file: str):
    """
    Упрощенная версия для быстрого объединения
    """
    # Загружаем данные
    df = pd.read_csv(csv_file)

    with open(json_file, 'r', encoding='utf-8') as f:
        json_data = json.load(f)

    # Создаем индекс JSON по ISIN коду
    json_index = {}
    for company in json_data:
        isin = company.get('ISIN Code')
        if isin:
            json_index[isin] = company

    # Объединяем данные
    result = []
    for _, row in df.iterrows():
        merged = dict(row)  # Копируем все данные из CSV

        # Добавляем данные из JSON по тикеру
        ticker = row.get('Ticker')
        if ticker in json_index:
            json_company = json_index[ticker]
            # Добавляем поля из JSON
            merged.update({
                'Registered_Office_JSON': json_company.get('Registered Office'),
                'Telephone_JSON': json_company.get('Telephone'),
                'Email_JSON': json_company.get('Email'),
                'Website_JSON': json_company.get('Link to Internet Website'),
                'Business_Fields_JSON': json_company.get('Main Business Fields')
            })

        result.append(merged)

    # Сохраняем результат
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"Быстрое объединение завершено!")
    print(f"Файл сохранен: {output_file}")


def main():
    # Конфигурация файлов
    CSV_FILE = "Indonesian_companies.csv"  # Ваш CSV файл с финансовыми данными
    JSON_FILE = "idx_stocks.json"  # Ваш JSON файл с детальной информацией
    OUTPUT_FILE = "merged_companies_data.json"

    print("Начинаем объединение CSV и JSON данных...")

    # Вариант 1: Полное объединение с интеллектуальным поиском
    load_and_merge_data(CSV_FILE, JSON_FILE, OUTPUT_FILE)

    # Загружаем результат для статистики
    with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
        merged_data = json.load(f)

    generate_statistics(merged_data)

    # Вариант 2: Быстрое объединение (раскомментируйте если нужно)
    # QUICK_OUTPUT = "quick_merged_data.json"
    # quick_merge(CSV_FILE, JSON_FILE, QUICK_OUTPUT)


if __name__ == "__main__":
    main()
