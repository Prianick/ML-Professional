import csv
import sqlite3
from pathlib import Path

# CSV_PATH = Path(__file__).resolve().parent.parent / "dataset" / "WA_Fn-UseC_-Telco-Customer-Churn.csv"
CSV_PATH = Path(__file__).resolve().parent.parent / "dataset" / "WA_Fn-UseC_-Marketing-Customer-Value-Analysis.csv"
SQLITE_PATH = Path(__file__).resolve().parent.parent / "dataset" / "churn.sqlite"
TABLE_NAME = "watson_churn"


def quote_identifier(name: str) -> str:
    return f"\"{name.replace('\"', '\"\"')}\""


def infer_column_type(values):
    has_float = False
    for value in values:
        if value is None or value == "":
            continue
        try:
            int(value)
            continue
        except ValueError:
            pass
        try:
            float(value)
            has_float = True
            continue
        except ValueError:
            return "TEXT"
    return "REAL" if has_float else "INTEGER"


def main():
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

    with CSV_PATH.open(newline="", encoding="utf-8") as file_handle:
        reader = csv.reader(file_handle)
        header = next(reader)
        rows = [row for row in reader]

    if not header:
        raise ValueError("CSV has no header")

    # Normalize row lengths
    rows = [row + [""] * (len(header) - len(row)) for row in rows]

    # Infer column types from data
    columns = []
    for column_index, column_name in enumerate(header):
        column_values = [row[column_index] if column_index < len(row) else "" for row in rows]
        column_type = infer_column_type(column_values)
        columns.append((column_name, column_type))

    create_columns = ", ".join(
        f"{quote_identifier(name)} {column_type}" for name, column_type in columns
    )
    quoted_table_name = quote_identifier(TABLE_NAME)

    connection = sqlite3.connect(SQLITE_PATH)
    try:
        connection.execute(
            f"CREATE TABLE IF NOT EXISTS {quoted_table_name} ({create_columns})"
        )

        placeholders = ", ".join(["?"] * len(header))
        insert_sql = (
            f"INSERT INTO {quoted_table_name} "
            f"({', '.join(quote_identifier(header_name) for header_name in header)}) "
            f"VALUES ({placeholders})"
        )

        prepared_rows = [
            [None if value == "" else value for value in row[: len(header)]]
            for row in rows
        ]
        connection.executemany(insert_sql, prepared_rows)
        connection.commit()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
