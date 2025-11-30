#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/02_prepare_data.py

Скрипт для подготовки данных опроса для загрузки в BigQuery.
Создает:
1. demographics.csv - демографические данные
2. 8 unpivot таблиц для технологий (Language, Database, Platform, Webframe)
"""

import pandas as pd
import numpy as np
from pathlib import Path
import os
from datetime import datetime

# ============================================================================
# КОНСТАНТЫ И НАСТРОЙКИ
# ============================================================================

INPUT_FILE = 'data/raw/survey_results.csv'
OUTPUT_DIR = 'data/processed'

# Технологические столбцы (из вашего анализа)
TECH_COLUMNS_MAP = {
    'LanguageHaveWorkedWith': ('language', 'haveworked'),
    'LanguageWantToWorkWith': ('language', 'wanttowork'),
    'DatabaseHaveWorkedWith': ('database', 'haveworked'),
    'DatabaseWantToWorkWith': ('database', 'wanttowork'),
    'PlatformHaveWorkedWith': ('platform', 'haveworked'),
    'PlatformWantToWorkWith': ('platform', 'wanttowork'),
    'WebframeHaveWorkedWith': ('webframe', 'haveworked'),
    'WebframeWantToWorkWith': ('webframe', 'wanttowork')
}

# Демографические столбцы (ключевые для анализа)
DEMO_COLUMNS = [
    'ResponseId',
    'Country',
    'Age',
    'EdLevel',
    'YearsCode',
    'YearsCodePro',
    'Employment',
    'RemoteWork',
    'DevType',
    'OrgSize'
]

# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================

def print_header(text):
    """Печать красивого заголовка"""
    print("\n" + "="*70)
    print(text)
    print("="*70)

def print_subheader(text):
    """Печать подзаголовка"""
    print("\n" + "─"*70)
    print(text)
    print("─"*70)

def safe_strip(value):
    """Безопасное удаление пробелов"""
    if pd.isna(value):
        return None
    return str(value).strip()

# ============================================================================
# ОСНОВНЫЕ ФУНКЦИИ
# ============================================================================

def load_data(filepath):
    """Загрузка исходных данных"""
    print_header("📂 ЗАГРУЗКА ИСХОДНЫХ ДАННЫХ")
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Файл '{filepath}' не найден!")
    
    print(f"Файл: {filepath}")
    df = pd.read_csv(filepath, low_memory=False)
    print(f"✓ Загружено строк: {len(df):,}")
    print(f"✓ Столбцов: {len(df.columns):,}")
    
    return df

def create_demographics_table(df):
    """
    Создание таблицы с демографическими данными
    """
    print_header("👥 СОЗДАНИЕ ТАБЛИЦЫ DEMOGRAPHICS")
    
    # Проверка наличия всех столбцов
    available_columns = [col for col in DEMO_COLUMNS if col in df.columns]
    missing_columns = [col for col in DEMO_COLUMNS if col not in df.columns]
    
    print(f"\n✓ Доступно столбцов: {len(available_columns)}/{len(DEMO_COLUMNS)}")
    if missing_columns:
        print(f"⚠️  Отсутствующие столбцы: {', '.join(missing_columns)}")
    
    # Создаем копию с доступными столбцами
    demo_df = df[available_columns].copy()
    
    # Обработка пропущенных значений
    print("\n🔧 Обработка пропущенных значений...")
    
    # Для каждого столбца (кроме ResponseId) создаем флаг валидности
    for col in available_columns:
        if col == 'ResponseId':
            continue
        
        # Создаем флаг валидности
        is_valid_col = f"{col}_IsValid"
        demo_df[is_valid_col] = demo_df[col].notna() & (demo_df[col].astype(str).str.strip() != '')
        
        # Заменяем пропуски на "Not Specified"
        demo_df[col] = demo_df[col].fillna('Not Specified')
        demo_df[col] = demo_df[col].replace('', 'Not Specified')
        
        # Статистика
        valid_count = demo_df[is_valid_col].sum()
        valid_percent = (valid_count / len(demo_df) * 100)
        print(f"  • {col}: {valid_count:,}/{len(demo_df):,} валидных ({valid_percent:.1f}%)")
    
    # Добавляем метаданные
    demo_df['CreatedAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    print(f"\n✓ Итоговая таблица: {len(demo_df):,} строк × {len(demo_df.columns)} столбцов")
    
    return demo_df

def create_technology_unpivot_table(df, source_column, tech_type, status):
    """
    Создание unpivot таблицы для конкретной технологии
    
    Args:
        df: исходный DataFrame
        source_column: название столбца с технологиями (например, 'LanguageHaveWorkedWith')
        tech_type: тип технологии (например, 'language')
        status: статус (например, 'haveworked')
    
    Returns:
        DataFrame с развернутыми технологиями
    """
    print_subheader(f"🔨 Обработка: {source_column}")
    
    # Проверка наличия столбца
    if source_column not in df.columns:
        print(f"⚠️  Столбец '{source_column}' не найден, пропускаем")
        return None
    
    # Статистика исходных данных
    total_rows = len(df)
    null_count = df[source_column].isna().sum()
    valid_count = total_rows - null_count
    
    print(f"  Всего строк: {total_rows:,}")
    print(f"  Валидных данных: {valid_count:,} ({valid_count/total_rows*100:.1f}%)")
    
    # Фильтруем валидные данные
    valid_data = df[df[source_column].notna()].copy()
    valid_data = valid_data[valid_data[source_column].astype(str).str.strip() != '']
    
    if len(valid_data) == 0:
        print(f"  ⚠️  Нет валидных данных для обработки")
        return None
    
    # Создаем unpivot таблицу
    unpivot_records = []
    
    for idx, row in valid_data.iterrows():
        response_id = row['ResponseId']
        tech_string = str(row[source_column])
        
        # Разделяем технологии по ";"
        technologies = [tech.strip() for tech in tech_string.split(';') if tech.strip()]
        
        # Создаем запись для каждой технологии
        for tech in technologies:
            unpivot_records.append({
                'ResponseId': response_id,
                'Technology': tech
            })
    
    # Создаем DataFrame
    unpivot_df = pd.DataFrame(unpivot_records)
    
    # Удаляем дубликаты (если респондент указал одну технологию дважды)
    before_dedup = len(unpivot_df)
    unpivot_df = unpivot_df.drop_duplicates(subset=['ResponseId', 'Technology'])
    after_dedup = len(unpivot_df)
    
    if before_dedup > after_dedup:
        print(f"  ⚠️  Удалено дубликатов: {before_dedup - after_dedup:,}")
    
    # Статистика результата
    unique_respondents = unpivot_df['ResponseId'].nunique()
    unique_technologies = unpivot_df['Technology'].nunique()
    avg_tech_per_respondent = len(unpivot_df) / unique_respondents
    
    print(f"  ✓ Создано записей: {len(unpivot_df):,}")
    print(f"  ✓ Уникальных респондентов: {unique_respondents:,}")
    print(f"  ✓ Уникальных технологий: {unique_technologies:,}")
    print(f"  ✓ Среднее технологий на респондента: {avg_tech_per_respondent:.1f}")
    
    # Топ-5 технологий для проверки
    top_5 = unpivot_df['Technology'].value_counts().head(5)
    print(f"\n  Топ-5 технологий:")
    for tech, count in top_5.items():
        print(f"    {count:>5,} - {tech}")
    
    return unpivot_df

def save_table(df, filename, output_dir):
    """Сохранение таблицы в CSV"""
    if df is None or len(df) == 0:
        print(f"  ⚠️  Таблица пустая, пропускаем сохранение: {filename}")
        return None
    
    filepath = os.path.join(output_dir, filename)
    df.to_csv(filepath, index=False, encoding='utf-8')
    
    # Размер файла
    file_size = os.path.getsize(filepath) / 1024  # KB
    
    print(f"  ✓ Сохранено: {filename}")
    print(f"    Строк: {len(df):,}")
    print(f"    Столбцов: {len(df.columns)}")
    print(f"    Размер: {file_size:.1f} KB")
    
    return filepath

def validate_data_integrity(df_original, created_files):
    """
    Валидация целостности созданных данных
    """
    print_header("🔍 ВАЛИДАЦИЯ ЦЕЛОСТНОСТИ ДАННЫХ")
    
    total_respondents = len(df_original)
    print(f"\nИсходное количество респондентов: {total_respondents:,}")
    
    # Проверка demographics
    demo_file = os.path.join(OUTPUT_DIR, 'demographics.csv')
    if os.path.exists(demo_file):
        demo_df = pd.read_csv(demo_file)
        demo_count = len(demo_df)
        
        if demo_count == total_respondents:
            print(f"✓ demographics.csv: {demo_count:,} строк (совпадает)")
        else:
            print(f"⚠️  demographics.csv: {demo_count:,} строк (ожидалось {total_respondents:,})")
    
    # Проверка технологических таблиц
    print("\nПроверка технологических таблиц:")
    for tech_file in created_files:
        if 'demographics' in tech_file:
            continue
        
        tech_df = pd.read_csv(tech_file)
        unique_respondents = tech_df['ResponseId'].nunique()
        total_records = len(tech_df)
        
        filename = os.path.basename(tech_file)
        print(f"\n  {filename}:")
        print(f"    Всего записей: {total_records:,}")
        print(f"    Уникальных респондентов: {unique_respondents:,}")
        print(f"    Среднее на респондента: {total_records/unique_respondents:.1f}")
        
        # Проверка на респондентов, которых нет в исходной таблице
        original_ids = set(df_original['ResponseId'])
        tech_ids = set(tech_df['ResponseId'])
        missing_ids = tech_ids - original_ids
        
        if missing_ids:
            print(f"    ⚠️  Найдено {len(missing_ids)} ID не из исходной таблицы!")
        else:
            print(f"    ✓ Все ResponseId валидны")

def create_summary_report(created_files):
    """Создание итогового отчета"""
    print_header("📄 СОЗДАНИЕ ИТОГОВОГО ОТЧЕТА")
    
    report_lines = []
    report_lines.append("="*70)
    report_lines.append("ОТЧЕТ ПО ПОДГОТОВКЕ ДАННЫХ ДЛЯ BIGQUERY")
    report_lines.append("="*70)
    report_lines.append(f"\nДата создания: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append(f"\nСоздано файлов: {len(created_files)}")
    report_lines.append("\n" + "-"*70)
    report_lines.append("СПИСОК СОЗДАННЫХ ФАЙЛОВ:")
    report_lines.append("-"*70)
    
    total_size = 0
    for filepath in created_files:
        filename = os.path.basename(filepath)
        file_size = os.path.getsize(filepath) / 1024  # KB
        total_size += file_size
        
        df = pd.read_csv(filepath)
        rows = len(df)
        cols = len(df.columns)
        
        report_lines.append(f"\n{filename}:")
        report_lines.append(f"  Строк: {rows:,}")
        report_lines.append(f"  Столбцов: {cols}")
        report_lines.append(f"  Размер: {file_size:.1f} KB")
    
    report_lines.append("\n" + "-"*70)
    report_lines.append(f"ИТОГО: {total_size:.1f} KB ({total_size/1024:.2f} MB)")
    report_lines.append("="*70)
    
    report_text = "\n".join(report_lines)
    
    # Сохранение отчета
    report_path = os.path.join(OUTPUT_DIR, 'data_preparation_report.txt')
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report_text)
    
    print(report_text)
    print(f"\n✓ Отчет сохранен: {report_path}")

# ============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# ============================================================================

def main():
    """Основная функция выполнения"""
    
    print("\n" + "="*70)
    print("🚀 ПОДГОТОВКА ДАННЫХ ДЛЯ BIGQUERY")
    print("="*70)
    print(f"Время начала: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Создание выходной директории
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    
    created_files = []
    
    try:
        # ===== ШАГ 1: ЗАГРУЗКА ДАННЫХ =====
        df = load_data(INPUT_FILE)
        
        # ===== ШАГ 2: СОЗДАНИЕ DEMOGRAPHICS =====
        demo_df = create_demographics_table(df)
        demo_file = save_table(demo_df, 'demographics.csv', OUTPUT_DIR)
        if demo_file:
            created_files.append(demo_file)
        
        # ===== ШАГ 3: СОЗДАНИЕ ТЕХНОЛОГИЧЕСКИХ ТАБЛИЦ =====
        print_header("🔧 СОЗДАНИЕ ТЕХНОЛОГИЧЕСКИХ ТАБЛИЦ (UNPIVOT)")
        
        for source_column, (tech_type, status) in TECH_COLUMNS_MAP.items():
            # Создаем unpivot таблицу
            tech_df = create_technology_unpivot_table(df, source_column, tech_type, status)
            
            # Сохраняем
            if tech_df is not None:
                filename = f"{tech_type}_{status}.csv"
                tech_file = save_table(tech_df, filename, OUTPUT_DIR)
                if tech_file:
                    created_files.append(tech_file)
        
        # ===== ШАГ 4: ВАЛИДАЦИЯ =====
        validate_data_integrity(df, created_files)
        
        # ===== ШАГ 5: ИТОГОВЫЙ ОТЧЕТ =====
        create_summary_report(created_files)
        
        # ===== ЗАВЕРШЕНИЕ =====
        print_header("✅ ПОДГОТОВКА ДАННЫХ ЗАВЕРШЕНА УСПЕШНО!")
        print(f"\n📁 Все файлы сохранены в: {OUTPUT_DIR}/")
        print(f"📊 Создано файлов: {len(created_files)}")
        print(f"⏱️  Время завершения: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        print("\n" + "="*70)
        print("📝 СЛЕДУЮЩИЙ ШАГ:")
        print("   Запустите: python scripts/03_upload_to_bigquery.py")
        print("="*70)
        
        return 0
        
    except Exception as e:
        print_header("❌ ОШИБКА!")
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