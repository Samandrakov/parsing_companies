import json
import pandas as pd
from typing import List, Dict, Any
import re


def flatten_json_to_csv(json_file: str, csv_file: str):
    """
    Конвертирует JSON файл с объединенными данными в CSV
    """
    print("=== КОНВЕРТАЦИЯ JSON В CSV ===")

    # Загружаем JSON данные
    print("1. Загрузка JSON данных...")
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"   Загружено {len(data)} компаний из JSON")
    except Exception as e:
        print(f"   Ошибка загрузки JSON: {e}")
        return

    # Создаем плоскую структуру для CSV
    print("2. Подготовка данных для CSV...")
    flattened_data = []

    for company in data:
        flat_company = {}

        # Проходим по всем полям компании
        for key, value in company.items():
            # Обрабатываем вложенные структуры (если есть)
            if isinstance(value, (dict, list)):
                # Для словарей - объединяем ключи
                if isinstance(value, dict):
                    for sub_key, sub_value in value.items():
                        flat_key = f"{key}_{sub_key}"
                        flat_company[flat_key] = clean_value(sub_value)
                # Для списков - объединяем в строку
                elif isinstance(value, list):
                    flat_company[key] = "; ".join(str(item) for item in value)
            else:
                flat_company[key] = clean_value(value)

        flattened_data.append(flat_company)

    # Создаем DataFrame
    print("3. Создание DataFrame...")
    df = pd.DataFrame(flattened_data)

    # Упорядочиваем колонки для лучшей читаемости
    df = reorder_columns(df)

    # Сохраняем в CSV
    print("4. Сохранение в CSV...")
    df.to_csv(csv_file, index=False, encoding='utf-8-sig')  # utf-8-sig для корректного отображения в Excel

    print(f"\n=== РЕЗУЛЬТАТ ===")
    print(f"CSV файл сохранен: {csv_file}")
    print(f"Количество компаний: {len(df)}")
    print(f"Количество колонок: {len(df.columns)}")


def clean_value(value: Any) -> str:
    """
    Очищает значения для CSV
    """
    if value is None:
        return ""
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, str):
        # Убираем лишние пробелы и переносы строк
        cleaned = re.sub(r'\s+', ' ', value.strip())
        return cleaned
    else:
        return str(value)


def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Упорядочивает колонки для лучшей читаемости
    """
    # Приоритетные колонки (идут первыми)
    priority_columns = [
        'Ticker', 'Company_name', 'Full_Company_Name', 'ISIN_Code',
        'Market_Cap', 'Stock_price', 'Revenue', 'Net_Income', 'Revenue_Growth'
    ]

    # Финансовые колонки
    financial_columns = [col for col in df.columns if any(word in col.lower() for word in
                                                          ['revenue', 'income', 'profit', 'cash', 'growth', 'price',
                                                           'volume', 'cap'])]

    # Контактные колонки
    contact_columns = [col for col in df.columns if any(word in col.lower() for word in
                                                        ['office', 'telephone', 'fax', 'email', 'website'])]

    # Отраслевые колонки
    industry_columns = [col for col in df.columns if any(word in col.lower() for word in
                                                         ['sector', 'industry', 'business', 'field'])]

    # Остальные колонки
    other_columns = [col for col in df.columns if col not in
                     priority_columns + financial_columns + contact_columns + industry_columns]

    # Собираем упорядоченный список колонок
    ordered_columns = []

    # Добавляем приоритетные
    for col in priority_columns:
        if col in df.columns:
            ordered_columns.append(col)

    # Добавляем финансовые
    for col in financial_columns:
        if col not in ordered_columns:
            ordered_columns.append(col)

    # Добавляем контактные
    for col in contact_columns:
        if col not in ordered_columns:
            ordered_columns.append(col)

    # Добавляем отраслевые
    for col in industry_columns:
        if col not in ordered_columns:
            ordered_columns.append(col)

    # Добавляем остальные
    for col in other_columns:
        if col not in ordered_columns:
            ordered_columns.append(col)

    return df[ordered_columns]


def create_summary_report(csv_file: str):
    """
    Создает краткий отчет о данных в CSV
    """
    print("\n=== ОТЧЕТ О ДАННЫХ ===")

    df = pd.read_csv(csv_file)

    print(f"Общее количество компаний: {len(df)}")
    print(f"Количество колонок: {len(df.columns)}")

    # Статистика по заполненности данных
    print(f"\nЗаполненность ключевых полей:")
    key_fields = ['Market_Cap', 'Revenue', 'Net_Income', 'Email', 'Website', 'Telephone']

    for field in key_fields:
        if field in df.columns:
            filled_count = df[field].notna().sum()
            percentage = (filled_count / len(df)) * 100
            print(f"  {field}: {filled_count}/{len(df)} ({percentage:.1f}%)")


def convert_specific_json_to_csv():
    """
    Версия для конкретного формата вашего JSON
    """
    input_json = "merged_companies_data.json"  # Ваш JSON файл
    output_csv = "final_companies_data.csv"

    print("Конвертация специфического JSON формата...")

    with open(input_json, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Создаем список всех возможных колонок
    all_columns = set()
    for company in data:
        all_columns.update(company.keys())

    # Создаем DataFrame
    df = pd.DataFrame(data)

    # Оптимизируем порядок колонок для вашего формата
    column_order = [
        # Основные идентификаторы
        'Ticker', 'ISIN_Code', 'Company_name', 'Full_Company_Name',

        # Финансовые показатели из CSV
        'Market_Cap', 'Stock_price', 'Volume', 'Revenue', 'Revenue_Growth',
        'Net_Income', 'Free_Cash_Flow',

        # Отраслевая информация
        'Industry_CSV', 'Sector_CSV', 'Industry_JSON', 'Sector_JSON',
        'Subsector', 'Sub_Industry', 'Main_Business_Fields',

        # Контактная информация
        'Registered_Office', 'Telephone', 'Fax', 'Email', 'Website',

        # Регистрационные данные
        'Taxpayer_Number', 'Securities_Administration_Bureau',
        'Recording_Date', 'Recording_Board',

        # Мета-данные
        'Data_Merge_Success', 'Data_Source'
    ]

    # Добавляем отсутствующие колонки в конец
    existing_columns = [col for col in column_order if col in df.columns]
    missing_columns = [col for col in df.columns if col not in existing_columns]

    final_columns = existing_columns + missing_columns
    df = df[final_columns]

    # Сохраняем
    df.to_csv(output_csv, index=False, encoding='utf-8-sig')

    print(f"Файл сохранен: {output_csv}")
    print(f"Компаний: {len(df)}")
    print(f"Колонок: {len(df.columns)}")

    return df


# Быстрая конвертация для немедленного использования
def quick_json_to_csv(json_file: str, csv_file: str):
    """
    Быстрая конвертация JSON в CSV
    """
    print("Быстрая конвертация JSON -> CSV...")

    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Просто конвертируем в DataFrame и сохраняем
    df = pd.DataFrame(data)
    df.to_csv(csv_file, index=False, encoding='utf-8-sig')

    print(f"✓ Готово! {len(data)} компаний сохранено в {csv_file}")
    print(f"✓ Колонки: {list(df.columns)}")


def main():
    """
    Основная функция с разными вариантами конвертации
    """
    # Конфигурация файлов
    JSON_FILE = "merged_companies_data.json"  # Ваш JSON файл
    CSV_FILE = "final_companies_data.csv"  # Выходной CSV файл

    print("Выберите вариант конвертации:")
    print("1. Полная конвертация с оптимизацией")
    print("2. Быстрая конвертация")
    print("3. Конвертация для специфического формата")

    choice = input("Введите номер (1-3): ").strip()

    if choice == "1":
        flatten_json_to_csv(JSON_FILE, CSV_FILE)
        create_summary_report(CSV_FILE)

    elif choice == "2":
        quick_json_to_csv(JSON_FILE, CSV_FILE)

    elif choice == "3":
        df = convert_specific_json_to_csv()
        print(f"\nПервые 5 колонок: {list(df.columns[:5])}")
        print(f"Всего колонок: {len(df.columns)}")

    else:
        print("Используется вариант по умолчанию: быстрая конвертация")
        quick_json_to_csv(JSON_FILE, CSV_FILE)


# Дополнительная утилита для просмотра структуры JSON
def analyze_json_structure(json_file: str):
    """
    Анализирует структуру JSON файла
    """
    print(f"\n=== АНАЛИЗ СТРУКТУРЫ JSON ===")

    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if not data:
        print("JSON файл пуст")
        return

    # Анализируем первую компанию
    first_company = data[0]
    print(f"Колонки в JSON ({len(first_company.keys())}):")

    for i, key in enumerate(first_company.keys(), 1):
        value = first_company[key]
        value_type = type(value).__name__
        value_preview = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
        print(f"  {i:2d}. {key}: {value_type} = {value_preview}")


if __name__ == "__main__":
    # Можно также просто запустить быструю конвертацию
    INPUT_JSON = "merged_companies_data.json"  # Ваш файл
    OUTPUT_CSV = "companies_final.csv"

    # Быстрая конвертация (раскомментируйте для использования)
    quick_json_to_csv(INPUT_JSON, OUTPUT_CSV)

    # Анализ структуры (раскомментируйте если нужно)
    # analyze_json_structure(INPUT_JSON)

    # Или используйте интерактивный режим
    # main()