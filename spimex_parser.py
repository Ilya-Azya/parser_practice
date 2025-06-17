import os
from datetime import datetime
from urllib.request import urlretrieve

import pandas as pd

from database import engine, SessionLocal
from models import SpimexTradingResult, Base

BASE_URL = "https://spimex.com/upload/reports/oil_xls/"
DOWNLOAD_DIR = "downloads"


def init_schema():
    Base.metadata.create_all(bind=engine)


def download_excel_file(file_name: str) -> str:
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    path = os.path.join(DOWNLOAD_DIR, file_name)
    if not os.path.exists(path):
        url = f"{BASE_URL}{file_name}?r=1404"
        urlretrieve(url, path)
    return path


def clean_col_name(col):
    parts = [str(p).strip() for p in col if 'Unnamed' not in str(p)]
    return ' '.join(parts).replace('\n', ' ')


def parse_excel(file_path: str, trade_date: datetime):
    df_all = pd.read_excel(file_path, sheet_name=None, header=[6, 7], index_col=0)

    for sheet, df in df_all.items():
        df.columns = [clean_col_name(col) for col in df.columns]

    for sheet_name, df in df_all.items():
        print(f"\nОбрабатываем лист: {sheet_name}")

        df.columns = df.columns.str.replace('\n', ' ').str.strip()
        df.columns = df.columns.str.replace('Обьем Договоров, руб.', 'Объем Договоров, руб.')

        required = [
            'Код Инструмента',
            'Наименование Инструмента',
            'Базис поставки',
            'Объем Договоров в единицах измерения',
            'Объем Договоров, руб.',
            'Количество Договоров, шт.'
        ]

        if not all(col in df.columns for col in required):
            print(f"В листе '{sheet_name}' отсутствуют необходимые столбцы")
            print("Столбцы листа:", list(df.columns))
            continue

        df_filtered = df[
            ['Код Инструмента', 'Наименование Инструмента', 'Базис поставки',
             'Объем Договоров в единицах измерения', 'Объем Договоров, руб.', 'Количество Договоров, шт.']
        ]

        df_filtered = df_filtered[
            pd.to_numeric(df_filtered['Количество Договоров, шт.'], errors='coerce').fillna(0) > 0]

        df_filtered['Объем Договоров в единицах измерения'] = pd.to_numeric(
            df_filtered['Объем Договоров в единицах измерения'], errors='coerce').fillna(0)
        df_filtered['Объем Договоров, руб.'] = pd.to_numeric(df_filtered['Объем Договоров, руб.'],
                                                             errors='coerce').fillna(0)
        df_filtered['Количество Договоров, шт.'] = pd.to_numeric(df_filtered['Количество Договоров, шт.'],
                                                                 errors='coerce').fillna(0).astype(int)

        session = SessionLocal()
        count_added = 0

        for _, row in df_filtered.iterrows():
            eid = str(row['Код Инструмента'])

            if eid.startswith('Итого'):
                continue

            volume = pd.to_numeric(row['Объем Договоров в единицах измерения'], errors='coerce')
            total = pd.to_numeric(row['Объем Договоров, руб.'], errors='coerce')
            count = pd.to_numeric(row['Количество Договоров, шт.'], errors='coerce')

            if pd.isna(volume) or pd.isna(total) or pd.isna(count):
                continue

            obj = SpimexTradingResult(
                exchange_product_id=eid,
                exchange_product_name=row['Наименование Инструмента'],
                oil_id=eid[:4],
                delivery_basis_id=eid[4:7],
                delivery_basis_name=row['Базис поставки'],
                delivery_type_id=eid[-1],
                volume=float(volume),
                total=float(total),
                count=int(count),
                date=trade_date.date()
            )
            session.add(obj)
            count_added += 1

        session.commit()
        session.close()

        print(f"Добавлено записей из файла {os.path.basename(file_path)}: {count_added}")


def main():
    init_schema()

    for year in [2023, 2024, 2025]:
        for month in range(1, 13):
            for day in range(1, 32):
                try:
                    date = datetime(year, month, day)
                    fname = f"oil_xls_{date.strftime('%Y%m%d')}162000.xls"
                    path = download_excel_file(fname)
                    parse_excel(path, date)
                    print(f"[✓] Обработано: {date.date()}")
                except Exception as e:
                    print(f"[ ] Пропущено: {date.date()} — {e}")


if __name__ == "__main__":
    main()
