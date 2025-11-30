#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/03_upload_to_bigquery.py

Скрипт для загрузки подготовленных данных в BigQuery
"""

import os
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime
import time

# ============================================================================
# НАСТРОЙКИ
# ============================================================================

# Загрузка переменных окружения
load_dotenv()

# Установка credentials
CREDENTIALS_PATH = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
PROJECT_ID = os.getenv('GCP_PROJECT_ID')
DATASET_ID = os.getenv('BIGQUERY_DATASET', 'tech_survey_data')

# Путь к подготовленным данным
DATA_DIR = 'data/processed'

# Список файлов для загрузки
FILES_TO_UPLOAD = [
    'demographics.csv',
    'language_haveworked.csv',
    'language_wanttowork.csv',
    'database_haveworked.csv',
    'database_wanttowork.csv',
    'platform_haveworked.csv',
    'platform_wanttowork.csv',
    'webframe_haveworked.csv',
    'webframe_wanttowork.csv'
]

# Схемы таблиц
TABLE_SCHEMAS = {
    'demographics': [
        bigquery.SchemaField("ResponseId", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("Country", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Age", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("EdLevel", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("YearsCode", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("YearsCodePro", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Employment", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("RemoteWork", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("DevType", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("OrgSize", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Country_IsValid", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("Age_IsValid", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("EdLevel_IsValid", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("YearsCode_IsValid", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("YearsCodePro_IsValid", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("Employment_IsValid", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("RemoteWork_IsValid", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("DevType_IsValid", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("OrgSize_IsValid", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("CreatedAt", "TIMESTAMP", mode="NULLABLE"),
    ],
    'technology': [
        bigquery.SchemaField("ResponseId", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("Technology", "STRING", mode="REQUIRED"),
    ]
}

# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================

def print_header(text):
    """Печать заголовка"""
    print("\n" + "="*70)
    print(text)
    print("="*70)

def print_subheader(text):
    """Печать подзаголовка"""
    print("\n" + "─"*70)
    print(text)
    print("─"*70)

def check_credentials():
    """Проверка credentials"""
    print_header("🔐 ПРОВЕРКА CREDENTIALS")
    
    if not os.path.exists(CREDENTIALS_PATH):
        raise FileNotFoundError(f"Credentials файл не найден: {CREDENTIALS_PATH}")
    
    print(f"✓ Credentials файл: {CREDENTIALS_PATH}")
    
    # Установка переменной окружения
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = CREDENTIALS_PATH
    print(f"✓ Переменная окружения установлена")

def init_bigquery_client():
    """Инициализация BigQuery клиента"""
    print_header("🔌 ПОДКЛЮЧЕНИЕ К BIGQUERY")
    
    print(f"Project ID: {PROJECT_ID}")
    print(f"Dataset ID: {DATASET_ID}")
    
    try:
        client = bigquery.Client(project=PROJECT_ID)
        
        # Проверка подключения
        query = "SELECT 1 as test"
        result = client.query(query).result()
        
        print("✓ Подключение успешно")
        return client
        
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        raise

def check_dataset_exists(client, dataset_id):
    """Проверка существования dataset"""
    print_subheader(f"📊 Проверка dataset: {dataset_id}")
    
    try:
        dataset_ref = f"{PROJECT_ID}.{dataset_id}"
        dataset = client.get_dataset(dataset_ref)
        print(f"✓ Dataset существует: {dataset_ref}")
        print(f"  Location: {dataset.location}")
        print(f"  Created: {dataset.created}")
        return True
        
    except NotFound:
        print(f"❌ Dataset не найден: {dataset_ref}")
        print("\nСоздайте dataset вручную:")
        print("1. Откройте https://console.cloud.google.com/bigquery")
        print(f"2. Создайте dataset с именем: {dataset_id}")
        print("3. Location: US (или EU)")
        return False

def get_table_schema(table_name):
    """Получение схемы для таблицы"""
    if table_name == 'demographics':
        return TABLE_SCHEMAS['demographics']
    else:
        return TABLE_SCHEMAS['technology']

def create_or_replace_table(client, dataset_id, table_name, schema):
    """Создание или замена таблицы"""
    table_id = f"{PROJECT_ID}.{dataset_id}.{table_name}"
    
    # Удаляем таблицу если существует
    try:
        client.delete_table(table_id)
        print(f"  ⚠️  Существующая таблица удалена")
    except NotFound:
        pass
    
    # Создаем новую таблицу
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    print(f"  ✓ Таблица создана: {table_name}")
    
    return table

def upload_csv_to_bigquery(client, dataset_id, table_name, csv_path):
    """
    Загрузка CSV файла в BigQuery таблицу
    """
    print_subheader(f"📤 Загрузка: {table_name}")
    
    # Проверка существования файла
    if not os.path.exists(csv_path):
        print(f"  ❌ Файл не найден: {csv_path}")
        return False
    
    # Информация о файле
    file_size = os.path.getsize(csv_path) / 1024  # KB
    df = pd.read_csv(csv_path)
    print(f"  Файл: {os.path.basename(csv_path)}")
    print(f"  Размер: {file_size:.1f} KB")
    print(f"  Строк: {len(df):,}")
    print(f"  Столбцов: {len(df.columns)}")
    
    # Получение схемы
    schema = get_table_schema(table_name)
    
    # Создание таблицы
    table = create_or_replace_table(client, dataset_id, table_name, schema)
    table_id = f"{PROJECT_ID}.{dataset_id}.{table_name}"
    
    # Настройка job для загрузки
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Пропускаем заголовок
        autodetect=False,  # Используем явную схему
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Перезаписываем
        allow_quoted_newlines=True,  # Разрешаем переносы строк в кавычках
        max_bad_records=10  # Максимум плохих строк
    )
    
    # Загрузка данных
    print(f"  🔄 Загрузка данных в BigQuery...")
    start_time = time.time()
    
    try:
        with open(csv_path, "rb") as source_file:
            job = client.load_table_from_file(
                source_file,
                table_id,
                job_config=job_config
            )
        
        # Ожидание завершения job
        job.result()
        
        elapsed_time = time.time() - start_time
        
        # Проверка результата
        table = client.get_table(table_id)
        
        print(f"  ✓ Загрузка завершена за {elapsed_time:.1f} сек")
        print(f"  ✓ Загружено строк: {table.num_rows:,}")
        
        if job.errors:
            print(f"  ⚠️  Ошибок при загрузке: {len(job.errors)}")
            for error in job.errors[:5]:  # Показываем первые 5 ошибок
                print(f"    - {error}")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Ошибка загрузки: {e}")
        
        if hasattr(e, 'errors') and e.errors:
            print(f"\n  Детали ошибок:")
            for error in e.errors[:5]:
                print(f"    {error}")
        
        return False

def verify_uploaded_data(client, dataset_id, table_name, expected_rows):
    """Проверка загруженных данных"""
    table_id = f"{PROJECT_ID}.{dataset_id}.{table_name}"
    
    try:
        # Получаем информацию о таблице
        table = client.get_table(table_id)
        actual_rows = table.num_rows
        
        # Проверяем количество строк
        if actual_rows == expected_rows:
            print(f"    ✓ Количество строк совпадает: {actual_rows:,}")
        else:
            print(f"    ⚠️  Несоответствие строк: {actual_rows:,} (ожидалось {expected_rows:,})")
        
        # Получаем несколько строк для проверки
        query = f"""
            SELECT *
            FROM `{table_id}`
            LIMIT 3
        """
        
        result = client.query(query).result()
        df_sample = result.to_dataframe()
        
        print(f"    ✓ Данные доступны для запросов")
        print(f"\n    Пример данных (первые 3 строки):")
        print(df_sample.to_string(index=False))
        
        return True
        
    except Exception as e:
        print(f"    ❌ Ошибка проверки: {e}")
        return False

def create_summary_report(results):
    """Создание итогового отчета"""
    print_header("📊 ИТОГОВЫЙ ОТЧЕТ")
    
    successful = [r for r in results if r['success']]
    failed = [r for r in results if not r['success']]
    
    print(f"\nУспешно загружено: {len(successful)}/{len(results)}")
    
    if successful:
        print("\n✓ Успешные загрузки:")
        for result in successful:
            print(f"  • {result['table_name']}: {result['rows']:,} строк")
    
    if failed:
        print("\n❌ Неудачные загрузки:")
        for result in failed:
            print(f"  • {result['table_name']}: {result['error']}")
    
    # Общая статистика
    total_rows = sum(r['rows'] for r in successful)
    print(f"\n📊 Всего загружено строк: {total_rows:,}")
    
    return len(failed) == 0

# ============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# ============================================================================

def main():
    """Основная функция"""
    
    print("\n" + "="*70)
    print("🚀 ЗАГРУЗКА ДАННЫХ В BIGQUERY")
    print("="*70)
    print(f"Время начала: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    results = []
    
    try:
        # ===== ШАГ 1: ПРОВЕРКА CREDENTIALS =====
        check_credentials()
        
        # ===== ШАГ 2: ПОДКЛЮЧЕНИЕ К BIGQUERY =====
        client = init_bigquery_client()
        
        # ===== ШАГ 3: ПРОВЕРКА DATASET =====
        if not check_dataset_exists(client, DATASET_ID):
            print("\n❌ Сначала создайте dataset!")
            return 1
        
        # ===== ШАГ 4: ЗАГРУЗКА ФАЙЛОВ =====
        print_header("📤 ЗАГРУЗКА ДАННЫХ")
        
        for filename in FILES_TO_UPLOAD:
            # Определяем имя таблицы (без расширения .csv)
            table_name = filename.replace('.csv', '')
            csv_path = os.path.join(DATA_DIR, filename)
            
            # Загружаем файл
            success = upload_csv_to_bigquery(client, DATASET_ID, table_name, csv_path)
            
            # Если загрузка успешна - проверяем данные
            if success:
                df = pd.read_csv(csv_path)
                expected_rows = len(df)
                
                print(f"\n  🔍 Проверка загруженных данных:")
                verify_uploaded_data(client, DATASET_ID, table_name, expected_rows)
                
                results.append({
                    'table_name': table_name,
                    'success': True,
                    'rows': expected_rows,
                    'error': None
                })
            else:
                results.append({
                    'table_name': table_name,
                    'success': False,
                    'rows': 0,
                    'error': 'Upload failed'
                })
        
        # ===== ШАГ 5: ИТОГОВЫЙ ОТЧЕТ =====
        all_success = create_summary_report(results)
        
        # ===== ЗАВЕРШЕНИЕ =====
        if all_success:
            print_header("✅ ВСЕ ДАННЫЕ ЗАГРУЖЕНЫ УСПЕШНО!")
            print(f"\n📊 Dataset: {PROJECT_ID}.{DATASET_ID}")
            print(f"🌐 BigQuery Console: https://console.cloud.google.com/bigquery")
            print(f"⏱️  Время завершения: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            print("\n" + "="*70)
            print("📝 СЛЕДУЮЩИЙ ШАГ:")
            print("   Создание SQL Views для дашборда")
            print("="*70)
            
            return 0
        else:
            print_header("⚠️  ЗАГРУЗКА ЗАВЕРШЕНА С ОШИБКАМИ")
            return 1
        
    except Exception as e:
        print_header("❌ КРИТИЧЕСКАЯ ОШИБКА!")
        print(f"\n{type(e).__name__}: {e}")
        
        import traceback
        print("\nПолный traceback:")
        print(traceback.format_exc())
        
        return 1

# ============================================================================
# ТОЧКА ВХОДА
# ============================================================================

if __name__ == "__main__":
    exit(main())