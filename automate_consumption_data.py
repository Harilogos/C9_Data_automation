"""
Automation script for processing consumption data as per 'Consumption_data_v2.ipynb'.

Steps:
1. Process consumption data of HRBR Unit: split datetime, calculate percentage, write to Excel.
2. Split consumption value of all units into hourly.
3. Consolidate all units (hourly).
4. Add ToD slot column into hourly data.
5. Merge hourly data to ToD slots.
6. Split hourly data into 15 mins interval.
7. Merge hourly data to daily data.
"""

import pandas as pd
import numpy as np
from openpyxl import load_workbook
from validation_utils import (
    validate_columns,
    validate_no_nans,
    validate_positive_values,
    validate_percentage_sum,
    validate_unique,
    validate_file_exists,
    validate_sheet_exists,
    validate_datetime_column,
    validate_nonempty,
)

def process_hrbr_consumption(input_file):
    # Step 1: Process consumption data of HRBR Unit
    validate_file_exists(input_file)
    df = pd.read_excel(input_file)
    validate_columns(df, ['DateTime', 'Consumption'], context="HRBR input")
    # Clean up: treat empty strings/whitespace as NaN in 'Consumption'
    df['Consumption'] = df['Consumption'].replace(r'^\s*$', np.nan, regex=True)
    validate_no_nans(df, ['DateTime', 'Consumption'], context="HRBR input")
    validate_positive_values(df, ['Consumption'], context="HRBR input")
    df['DateTime'] = pd.to_datetime(df['DateTime'])
    df['Date'] = df['DateTime'].dt.date
    df['Time'] = df['DateTime'].dt.time
    total_consumption = df['Consumption'].sum()
    df['Consumption_%'] = ((df['Consumption'] / total_consumption) * 100).round(2)
    validate_percentage_sum(df, 'Consumption_%', expected_sum=100, tolerance=1, context="HRBR With_Percentages")
    with pd.ExcelWriter(input_file, mode='a', if_sheet_exists='replace', engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='With_Percentages', index=False)

def split_monthly_to_hourly(total_value, hourly_percentages):
    hourly_percentages = np.array(hourly_percentages, dtype=float)
    normalized = hourly_percentages / hourly_percentages.sum()
    hourly_values = normalized * total_value
    return hourly_values

def split_units_to_hourly(input_file, output_file, unit_values):
    # Step 2: Split consumption value of all units into hourly
    validate_file_exists(input_file)
    validate_sheet_exists(input_file, "With_Percentages")
    df = pd.read_excel(input_file, sheet_name="With_Percentages")
    validate_columns(df, ["Consumption_%"], context="With_Percentages")
    validate_no_nans(df, ["Consumption_%"], context="With_Percentages")
    dates = pd.to_datetime(df.iloc[:, 0])
    hourly_percentages = df["Consumption_%"].tolist()
    zero_slots = pd.to_datetime([
        "2025-08-07 18:00:02",
        "2025-08-07 19:00:02",
        "2025-08-07 20:00:02"
    ])
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        for unit, total_value in unit_values.items():
            hourly_values = split_monthly_to_hourly(total_value, hourly_percentages)
            if unit.lower() == "hrbr unit":
                mask = dates.isin(zero_slots)
                hourly_values = pd.Series(hourly_values)
                hourly_values.loc[mask] = 0
            unit_df = pd.DataFrame({
                "Date": dates,
                "Consumption": hourly_values
            })
            unit_df.to_excel(writer, sheet_name=unit, index=False)

def consolidate_units_hourly(input_file, output_file):
    # Step 3: Consolidate all units (hourly)
    validate_file_exists(input_file)
    sheets_dict = pd.read_excel(input_file, sheet_name=None)
    merged_df = pd.DataFrame()
    for sheet_name, df in sheets_dict.items():
        if "Date" in df.columns and "Consumption" in df.columns:
            df["Unit"] = sheet_name
            df["DateTime"] = pd.to_datetime(df["Date"])
            df["Date"] = df["DateTime"].dt.date
            df["Time"] = df["DateTime"].dt.strftime("%H:00:00")
            df = df.drop(columns=["DateTime"])
            merged_df = pd.concat([merged_df, df], ignore_index=True)
    merged_df.to_excel(output_file, sheet_name="hourly", index=False)

def add_tod_slot(input_file):
    # Step 4: Add ToD slot column into hourly data
    validate_file_exists(input_file)
    validate_sheet_exists(input_file, "hourly")
    df = pd.read_excel(input_file, sheet_name="hourly")
    validate_columns(df, ["Date", "Time", "Consumption", "Unit"], context="hourly")
    validate_no_nans(df, ["Date", "Time", "Consumption"], context="hourly")
    df["DateTime"] = pd.to_datetime(df["Date"].astype(str) + " " + df["Time"].astype(str))
    df["Hour"] = df["DateTime"].dt.hour
    df["Date"] = df["DateTime"].dt.date
    def get_tod_slot(hour):
        if 22 <= hour <= 23 or 0 <= hour < 6:
            return "Night Off Peak"
        elif 6 <= hour < 9:
            return "Morning Peak"
        elif 9 <= hour < 18:
            return "Day Normal"
        elif 18 <= hour < 22:
            return "Evening Peak"
        else:
            return None
    df["ToD_Slot"] = df["Hour"].apply(get_tod_slot)
    with pd.ExcelWriter(input_file, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name="hourly1", index=False)

def merge_hourly_to_tod(input_file):
    # Step 5: Merge hourly data to ToD slots
    validate_file_exists(input_file)
    validate_sheet_exists(input_file, "hourly")
    df = pd.read_excel(input_file, sheet_name="hourly")
    validate_columns(df, ["Date", "Time", "Consumption", "Unit"], context="hourly")
    validate_no_nans(df, ["Date", "Time", "Consumption"], context="hourly")
    df["DateTime"] = pd.to_datetime(df["Date"].astype(str) + " " + df["Time"].astype(str))
    df["Hour"] = df["DateTime"].dt.hour
    df["Date"] = df["DateTime"].dt.date
    def get_tod_slot(hour):
        if 22 <= hour <= 23 or 0 <= hour < 6:
            return "Night Off Peak"
        elif 6 <= hour < 10:
            return "Morning Peak"
        elif 10 <= hour < 18:
            return "Day Normal"
        elif 18 <= hour < 22:
            return "Evening Peak"
        else:
            return None
    def get_time_range(tod_slot):
        time_ranges = {
            "Night Off Peak": "22:00 - 06:00",
            "Morning Peak": "06:00 - 09:00",
            "Day Normal": "09:00 - 18:00",
            "Evening Peak": "18:00 - 22:00"
        }
        return time_ranges.get(tod_slot, None)
    df["ToD_Slot"] = df["Hour"].apply(get_tod_slot)
    # Shift 22–23 hours to the NEXT day
    df_shift = df[df["Hour"].isin([22, 23])].copy()
    df_shift["Date"] = pd.to_datetime(df_shift["Date"]) + pd.Timedelta(days=1)
    df_shift["Date"] = df_shift["Date"].dt.date
    df = df[~df["Hour"].isin([22, 23])]
    df = pd.concat([df, df_shift], ignore_index=True)
    first_date = df["Date"].min()
    last_date = df["Date"].max()
    mask_last22 = (df["Date"] == last_date) & (df["Hour"].isin([22, 23]))
    df.loc[mask_last22, "Date"] = first_date
    tod_df = (
        df.groupby(["Date", "Unit", "ToD_Slot"], as_index=False)["Consumption"]
          .sum()
          .rename(columns={"Consumption": "Value"})
    )
    tod_df["Time"] = tod_df["ToD_Slot"].apply(get_time_range)
    column_order = ["Date", "Unit", "ToD_Slot", "Time", "Value"]
    tod_df = tod_df[column_order]
    with pd.ExcelWriter(input_file, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        tod_df.to_excel(writer, sheet_name="ToD", index=False)

def split_hourly_to_15min(input_file):
    # Step 6: Split hourly data into 15 mins interval
    validate_file_exists(input_file)
    validate_sheet_exists(input_file, "hourly1")
    df = pd.read_excel(input_file, sheet_name="hourly1")
    validate_columns(df, ["Date", "Time", "Consumption", "Unit"], context="hourly1")
    validate_no_nans(df, ["Date", "Time", "Consumption"], context="hourly1")
    df["DateTime"] = pd.to_datetime(df["Date"].astype(str) + " " + df["Time"].astype(str))
    expanded_rows = []
    for _, row in df.iterrows():
        base_time = row["DateTime"]
        consumption_per_15min = row["Consumption"] / 4
        for i in range(4):
            new_time = base_time + pd.Timedelta(minutes=15*i)
            expanded_rows.append({
                "Date": new_time.date(),
                "Time": new_time.time(),
                "Consumption": consumption_per_15min,
                "Unit": row["Unit"],
                "ToD_Slot": row['ToD_Slot']
            })
    df_15min = pd.DataFrame(expanded_rows)
    with pd.ExcelWriter(input_file, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        df_15min.to_excel(writer, sheet_name="15_mins", index=False)

def merge_hourly_to_daily(input_file):
    # Step 7: Merge hourly data to daily data
    validate_file_exists(input_file)
    validate_sheet_exists(input_file, "hourly")
    df = pd.read_excel(input_file, sheet_name="hourly")
    validate_columns(df, ["Date", "Time", "Consumption", "Unit"], context="hourly")
    validate_no_nans(df, ["Date", "Time", "Consumption"], context="hourly")
    df["DateTime"] = pd.to_datetime(df["Date"].astype(str) + " " + df["Time"].astype(str))
    df["Date"] = df["DateTime"].dt.date
    df_daily = df.groupby(["Date", "Unit"], as_index=False).agg({"Consumption": "sum"})
    with pd.ExcelWriter(input_file, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        df_daily.to_excel(writer, sheet_name="daily", index=False)

def main():
    hrbr_file = "HRBR Aug.xlsx"
    hourly_units_file = "hourly_consumption_units_Aug.xlsx"
    consolidated_file = "consumption_consolidated_aug.xlsx"

    unit_values = {
        "Malleswaram": 48359.985,
        "Electronic City": 69740,
        "Kanakapura": 45733.521,
        "Bellandur": 48752.24325,
        "Sarjapura": 45603.012,
        "Sahakar Nagar": 58407.5,
        "HRBR Unit": 45230,
        "Whitefield": 88540.058,
        "Bellandur Corp. Office": 22886.238,
        "Thanisandra": 53563.019,
        "Old Airport Road": 77528.014,
    }

    print("Step 1: Processing HRBR Unit consumption data...")
    process_hrbr_consumption(hrbr_file)
    print("Step 2: Splitting unit values to hourly...")
    split_units_to_hourly(hrbr_file, hourly_units_file, unit_values)
    print("Step 3: Consolidating all units (hourly)...")
    consolidate_units_hourly(hourly_units_file, consolidated_file)
    print("Step 4: Adding ToD slot column...")
    add_tod_slot(consolidated_file)
    print("Step 5: Merging hourly data to ToD slots...")
    merge_hourly_to_tod(consolidated_file)
    print("Step 6: Splitting hourly data into 15 min intervals...")
    split_hourly_to_15min(consolidated_file)
    print("Step 7: Merging hourly data to daily data...")
    merge_hourly_to_daily(consolidated_file)
    print("✅ All steps completed.")

if __name__ == "__main__":
    main()
