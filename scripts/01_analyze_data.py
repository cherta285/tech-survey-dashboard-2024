# scripts/01_analyze_data.py
"""
Скрипт для первичного анализа данных опроса
"""
import pandas as pd
import os
from pathlib import Path

print("="*70)
print("АНАЛИЗ ИСХОДНЫХ ДАННЫХ")
print("="*70)

# Путь к исходному файлу
INPUT_FILE = 'data/raw/survey_results.csv'

# Проверка существования файла
if not os.path.exists(INPUT_FILE):
    print(f"\n❌ ОШИБКА: Файл '{INPUT_FILE}' не найден!")
    print("\nПожалуйста:")
    print("1. Поместите ваш CSV файл в папку data/raw/")
    print("2. Переименуйте его в 'survey_results.csv'")
    print("3. Или измените переменную INPUT_FILE в этом скрипте")
    exit(1)

print(f"\n✓ Файл найден: {INPUT_FILE}")

# Загрузка данных
print("\n🔄 Загрузка данных...")
try:
    df = pd.read_csv(INPUT_FILE, low_memory=False)
    print(f"✓ Данные загружены успешно")
except Exception as e:
    print(f"❌ Ошибка при загрузке: {e}")
    exit(1)

# Основная информация
print("\n" + "="*70)
print("📊 ОСНОВНАЯ ИНФОРМАЦИЯ")
print("="*70)
print(f"Строк (респондентов): {len(df):,}")
print(f"Столбцов: {len(df.columns):,}")
print(f"Размер в памяти: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

# Список всех столбцов
print("\n" + "="*70)
print("📋 СПИСОК ВСЕХ СТОЛБЦОВ")
print("="*70)
for idx, col in enumerate(df.columns, 1):
    print(f"{idx:3d}. {col}")

# Поиск технологических столбцов
print("\n" + "="*70)
print("🔍 ПОИСК ТЕХНОЛОГИЧЕСКИХ СТОЛБЦОВ")
print("="*70)

tech_columns_patterns = [
    'Language', 'Database', 'Platform', 'Webframe', 'WebFrame'
]

found_tech_columns = []
for col in df.columns:
    for pattern in tech_columns_patterns:
        if pattern.lower() in col.lower():
            found_tech_columns.append(col)
            break

print(f"\nНайдено технологических столбцов: {len(found_tech_columns)}")
for col in found_tech_columns:
    non_null = df[col].notna().sum()
    null_percent = (df[col].isna().sum() / len(df) * 100)
    print(f"\n  • {col}")
    print(f"    Заполнено: {non_null:,} ({100-null_percent:.1f}%)")
    print(f"    Пропусков: {df[col].isna().sum():,} ({null_percent:.1f}%)")
    
    # Пример данных
    sample = df[col].dropna().iloc[0] if non_null > 0 else "Нет данных"
    if len(str(sample)) > 100:
        sample = str(sample)[:100] + "..."
    print(f"    Пример: {sample}")

# Поиск демографических столбцов
print("\n" + "="*70)
print("👥 ПОИСК ДЕМОГРАФИЧЕСКИХ СТОЛБЦОВ")
print("="*70)

demo_patterns = ['Country', 'Age', 'Ed', 'Gender', 'Employment', 'YearsCode']

found_demo_columns = []
for col in df.columns:
    for pattern in demo_patterns:
        if pattern.lower() in col.lower():
            found_demo_columns.append(col)
            break

print(f"\nНайдено демографических столбцов: {len(found_demo_columns)}")
for col in found_demo_columns:
    unique_vals = df[col].nunique()
    non_null = df[col].notna().sum()
    null_percent = (df[col].isna().sum() / len(df) * 100)
    
    print(f"\n  • {col}")
    print(f"    Уникальных значений: {unique_vals:,}")
    print(f"    Заполнено: {non_null:,} ({100-null_percent:.1f}%)")
    print(f"    Пропусков: {df[col].isna().sum():,} ({null_percent:.1f}%)")
    
    # Показываем первые 5 уникальных значений
    if unique_vals <= 20:
        top_values = df[col].value_counts().head(5)
        print(f"    Топ-5 значений:")
        for val, count in top_values.items():
            print(f"      - {val}: {count:,} ({count/len(df)*100:.1f}%)")

# Анализ пропущенных значений
print("\n" + "="*70)
print("🔍 АНАЛИЗ ПРОПУЩЕННЫХ ЗНАЧЕНИЙ")
print("="*70)

missing_data = pd.DataFrame({
    'Column': df.columns,
    'Missing_Count': df.isnull().sum(),
    'Missing_Percent': (df.isnull().sum() / len(df) * 100).round(2)
})

missing_data = missing_data[missing_data['Missing_Count'] > 0].sort_values(
    'Missing_Percent', ascending=False
)

if len(missing_data) > 0:
    print(f"\nСтолбцов с пропусками: {len(missing_data)}")
    print("\nТоп-10 столбцов с наибольшим количеством пропусков:")
    print(missing_data.head(10).to_string(index=False))
else:
    print("\n✓ Пропущенных значений не обнаружено!")

# Проверка наличия ResponseId
print("\n" + "="*70)
print("🔑 ПРОВЕРКА ИДЕНТИФИКАТОРА РЕСПОНДЕНТА")
print("="*70)

id_columns = ['ResponseId', 'RespondentId', 'Respondent', 'ID', 'id']
found_id = None

for col in id_columns:
    if col in df.columns:
        found_id = col
        break

if found_id:
    print(f"✓ Найден столбец ID: '{found_id}'")
    print(f"  Уникальных значений: {df[found_id].nunique():,}")
    print(f"  Дубликатов: {df[found_id].duplicated().sum():,}")
    
    if df[found_id].nunique() == len(df):
        print(f"  ✓ Все ID уникальны")
    else:
        print(f"  ⚠️ Есть дубликаты ID!")
else:
    print("⚠️ Столбец с ID респондента не найден")
    print("   Будет создан автоматически")

# Сохранение отчета
print("\n" + "="*70)
print("💾 СОХРАНЕНИЕ ОТЧЕТА")
print("="*70)

report_path = 'data/processed/data_analysis_report.txt'
Path('data/processed').mkdir(exist_ok=True)

with open(report_path, 'w', encoding='utf-8') as f:
    f.write("="*70 + "\n")
    f.write("ОТЧЕТ ПО АНАЛИЗУ ДАННЫХ\n")
    f.write("="*70 + "\n\n")
    f.write(f"Файл: {INPUT_FILE}\n")
    f.write(f"Строк: {len(df):,}\n")
    f.write(f"Столбцов: {len(df.columns):,}\n\n")
    
    f.write("ТЕХНОЛОГИЧЕСКИЕ СТОЛБЦЫ:\n")
    f.write("-"*70 + "\n")
    for col in found_tech_columns:
        f.write(f"  • {col}\n")
    
    f.write("\n\nДЕМОГРАФИЧЕСКИЕ СТОЛБЦЫ:\n")
    f.write("-"*70 + "\n")
    for col in found_demo_columns:
        f.write(f"  • {col}\n")
    
    if len(missing_data) > 0:
        f.write("\n\nПРОПУЩЕННЫЕ ЗНАЧЕНИЯ:\n")
        f.write("-"*70 + "\n")
        f.write(missing_data.head(20).to_string(index=False))

print(f"✓ Отчет сохранен: {report_path}")

# Итоговая сводка
print("\n" + "="*70)
print("✅ АНАЛИЗ ЗАВЕРШЕН")
print("="*70)
print(f"\n📊 Краткая сводка:")
print(f"  • Респондентов: {len(df):,}")
print(f"  • Технологических столбцов: {len(found_tech_columns)}")
print(f"  • Демографических столбцов: {len(found_demo_columns)}")
print(f"  • Столбцов с пропусками: {len(missing_data)}")
print(f"  • ID столбец: {found_id if found_id else 'Будет создан'}")

print("\n📝 Следующий шаг:")
print("   Запустите: python scripts/02_prepare_data.py")
print("="*70)