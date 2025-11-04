import pandas as pd
import numpy as np

def validate_columns(df, required_columns, context=""):
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing columns {missing} in {context}")

def validate_no_nans(df, columns, context=""):
    for col in columns:
        if df[col].isnull().any():
            raise ValueError(f"NaN values found in column '{col}' in {context}")

def validate_positive_values(df, columns, context=""):
    for col in columns:
        if (df[col] < 0).any():
            raise ValueError(f"Negative values found in column '{col}' in {context}")

def validate_percentage_sum(df, percentage_col, expected_sum=100, tolerance=0.5, context=""):
    total = df[percentage_col].sum()
    if not (expected_sum - tolerance <= total <= expected_sum + tolerance):
        raise ValueError(f"Sum of '{percentage_col}' is {total}, expected ~{expected_sum} in {context}")

def validate_unique(df, columns, context=""):
    if df.duplicated(subset=columns).any():
        raise ValueError(f"Duplicate rows found for columns {columns} in {context}")

def validate_file_exists(filepath):
    import os
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")

def validate_sheet_exists(filepath, sheet_name):
    import openpyxl
    wb = openpyxl.load_workbook(filepath, read_only=True)
    if sheet_name not in wb.sheetnames:
        raise ValueError(f"Sheet '{sheet_name}' not found in {filepath}")

def validate_datetime_column(df, column, context=""):
    try:
        pd.to_datetime(df[column])
    except Exception as e:
        raise ValueError(f"Invalid datetime in column '{column}' in {context}: {e}")

def validate_nonempty(df, context=""):
    if df.empty:
        raise ValueError(f"DataFrame is empty in {context}")
