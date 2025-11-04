"""
Automation script for processing generation data as per Generation_data_v2.ipynb.

Steps:
1. Merge inverter data for all days and inverters, handle invalid data, and save merged/invalid records.
2. Aggregate merged data to 15-minute intervals and save to a new sheet.
3. Split DateTime into Date and Time columns and save to another sheet.
4. Merge generation and consumption data, allocate generation to units by priority, and save the result.
5. Aggregate merged data to hourly granularity and save.
6. Format the date in the final CSV file.
"""

import os
import pandas as pd
import numpy as np
import warnings
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

def merge_inverter_data(base_folder, merged_file):
    warnings.simplefilter(action='ignore', category=pd.errors.SettingWithCopyWarning)
    merged_df = pd.DataFrame()
    invalid_dates_df = pd.DataFrame()
    invalid_gen_df = pd.DataFrame()

    for day in range(1, 32):
        day_str = str(day).zfill(2)
        day_folder = os.path.join(base_folder, day_str)
        for i in range(1, 17):
            file_name = f"KIDS_CLINIC__Inverter___INV_{i}(Day Data_{day_str}_08_2025)_.xlsx"
            file_path = os.path.join(day_folder, file_name)
            if os.path.exists(file_path):
                df = pd.read_excel(file_path, header=0)
                validate_columns(df, ['Date & Time', 'Day Gen (KWh)'], context=file_name)
                df['Source File'] = file_name
                df['Date & Time'] = pd.to_datetime(df['Date & Time'], errors='coerce', dayfirst=True)
                invalid_rows_date = df[df['Date & Time'].isna()]
                if not invalid_rows_date.empty:
                    invalid_dates_df = pd.concat([invalid_dates_df, invalid_rows_date], ignore_index=True)
                df['Day Gen (KWh)'] = pd.to_numeric(df['Day Gen (KWh)'], errors='coerce')
                invalid_rows_gen = df[df['Day Gen (KWh)'].isna()]
                if not invalid_rows_gen.empty:
                    invalid_gen_df = pd.concat([invalid_gen_df, invalid_rows_gen], ignore_index=True)
                df_valid = df.dropna(subset=['Date & Time', 'Day Gen (KWh)']).copy()
                validate_positive_values(df_valid, ['Day Gen (KWh)'], context=f"{file_name} valid rows")
                B = df_valid['Day Gen (KWh)'].values
                C = np.zeros_like(B)
                for j in range(len(B)-1):
                    C[j] = B[j] - B[j+1]
                C[-1] = B[-1]
                df_valid['Day Gen (KWh)'] = C
                df_valid = df_valid[['Date & Time', 'Day Gen (KWh)']]
                merged_df = pd.concat([merged_df, df_valid], ignore_index=True)
                print(f"✅ File processed : {file_path}")
            else:
                print(f"⚠️ File not found: {file_path}")

    merged_df = merged_df.sort_values('Date & Time').reset_index(drop=True)
    validate_nonempty(merged_df, context="Merged inverter data")
    merged_df.to_excel(merged_file, index=False)
    print(f"✅ All valid rows merged into {merged_file}")

    # Save merged generation data to a new Excel file for generation only
    generation_file = "Generation_Data_Aug.xlsx"
    merged_df.to_excel(generation_file, index=False)
    print(f"✅ Generation data also saved to {generation_file}")

    if not invalid_dates_df.empty:
        invalid_dates_df.to_excel(os.path.join(base_folder, "invalid_date_records.xlsx"), index=False)
        print(f"⚠️ Invalid Date & Time records saved to invalid_date_records.xlsx")
    if not invalid_gen_df.empty:
        invalid_gen_df.to_excel(os.path.join(base_folder, "invalid_daygen_records.xlsx"), index=False)
        print(f"⚠️ Non-numeric Day Gen records saved to invalid_daygen_records.xlsx")

def aggregate_15min(merged_file):
    validate_file_exists(merged_file)
    df = pd.read_excel(merged_file)
    validate_columns(df, ['Date & Time', 'Day Gen (KWh)'], context="Merged inverter data")
    validate_no_nans(df, ['Date & Time', 'Day Gen (KWh)'], context="Merged inverter data")
    df['DateTime'] = pd.to_datetime(df['Date & Time'])
    df = df.sort_values('DateTime')
    df_15min = (
        df.set_index('DateTime')
          .resample('15T')['Day Gen (KWh)']
          .sum()
          .reset_index()
    )
    validate_nonempty(df_15min, context="15min aggregated data")
    with pd.ExcelWriter(merged_file, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        df_15min.to_excel(writer, sheet_name='15min_Data', index=False)
    print("Aggregated 15-minute data saved to sheet '15min_Data'")

def split_date_time(merged_file):
    validate_file_exists(merged_file)
    validate_sheet_exists(merged_file, "15min_Data")
    df = pd.read_excel(merged_file, sheet_name="15min_Data")
    validate_columns(df, ['DateTime', 'Day Gen (KWh)'], context="15min_Data")
    validate_no_nans(df, ['DateTime', 'Day Gen (KWh)'], context="15min_Data")
    df['DateTime'] = pd.to_datetime(df['DateTime'], errors='coerce')
    df['Date'] = df['DateTime'].dt.date
    df['Time'] = df['DateTime'].dt.time
    with pd.ExcelWriter(merged_file, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        df.to_excel(writer, sheet_name='15 mins', index=False)
    print("✅ Data with split Date and Time saved to sheet '15 mins'")

def merge_generation_consumption(merged_file, consumption_file, output_file):
    priority_units = ["MALLESWARAM", "SAHAKAR NAGAR", "HRBR UNIT", "OLD AIRPORT ROAD"]
    validate_file_exists(merged_file)
    validate_sheet_exists(merged_file, "15 mins")
    validate_file_exists(consumption_file)
    validate_sheet_exists(consumption_file, "15_mins")
    gen_df = pd.read_excel(merged_file, sheet_name="15 mins")
    cons_df = pd.read_excel(consumption_file, sheet_name="15_mins")
    validate_columns(gen_df, ["Date", "Time", "Day Gen (KWh)"], context="Generation 15 mins")
    validate_columns(cons_df, ["Date", "Time", "Consumption", "Unit"], context="Consumption 15_mins")
    validate_no_nans(gen_df, ["Date", "Time", "Day Gen (KWh)"], context="Generation 15 mins")
    validate_no_nans(cons_df, ["Date", "Time", "Consumption", "Unit"], context="Consumption 15_mins")
    gen_df["Date"] = pd.to_datetime(gen_df["Date"]).dt.date
    gen_df["Time"] = pd.to_datetime(gen_df["Time"]).dt.time
    cons_df["Date"] = pd.to_datetime(cons_df["Date"]).dt.date
    cons_df["Time"] = pd.to_datetime(cons_df["Time"]).dt.time
    result_rows = []
    for (date, time), gen_row in gen_df.groupby(["Date", "Time"]):
        generation_available = gen_row["Day Gen (KWh)"].values[0]
        slot_df = cons_df[(cons_df["Date"] == date) & (cons_df["Time"] == time)].copy()
        slot_df["Generation_value"] = 0.0
        slot_df["Surplus_Generation"] = 0.0
        slot_df["Surplus_Demand"] = 0.0
        for unit in priority_units:
            if generation_available <= 0:
                break
            mask = slot_df["Unit"].str.upper() == unit.upper()
            if mask.any():
                cons_val = slot_df.loc[mask, "Consumption"].values[0]
                assigned = min(cons_val, generation_available)
                slot_df.loc[mask, "Generation_value"] = assigned
                if assigned < cons_val:
                    slot_df.loc[mask, "Surplus_Demand"] = cons_val - assigned
                generation_available -= assigned
        remaining = slot_df[~slot_df["Unit"].str.upper().isin([u.upper() for u in priority_units])]
        remaining = remaining.sort_values(by="Consumption", ascending=False)
        for idx, row in remaining.iterrows():
            if generation_available <= 0:
                slot_df.loc[idx, "Surplus_Demand"] = row["Consumption"]
                continue
            cons_val = row["Consumption"]
            assigned = min(cons_val, generation_available)
            slot_df.loc[idx, "Generation_value"] = assigned
            if assigned < cons_val:
                slot_df.loc[idx, "Surplus_Demand"] = cons_val - assigned
            generation_available -= assigned
        if generation_available > 0 and not slot_df.empty:
            last_idx = slot_df.index[-1]
            slot_df.loc[last_idx, "Surplus_Generation"] = generation_available
            generation_available = 0
        result_rows.append(slot_df)
    final_df = pd.concat(result_rows, ignore_index=True)
    final_df = final_df.rename(columns={
        "Consumption": "Consumption_value",
        "Location": "Unit",
    })
    final_df = final_df.fillna(0)
    final_df[[
        "Date", "Time", "Unit", "ToD_Slot",
        "Consumption_value", "Generation_value",
        "Surplus_Generation", "Surplus_Demand"
    ]].to_excel(output_file, index=False, sheet_name="15 mins")
    print(f"✅ File created: {output_file}")

def aggregate_hourly(input_file, output_file):
    validate_file_exists(input_file)
    df = pd.read_excel(input_file)
    validate_columns(df, ["Date", "Time", "Unit", "ToD_Slot", "Consumption_value", "Generation_value", "Surplus_Generation", "Surplus_Demand"], context="15 mins merged")
    validate_no_nans(df, ["Date", "Time", "Unit", "ToD_Slot", "Consumption_value", "Generation_value"], context="15 mins merged")
    df["DateTime"] = pd.to_datetime(df["Date"].astype(str) + " " + df["Time"].astype(str))
    df["Date"] = df["DateTime"].dt.date
    df["Time"] = df["DateTime"].dt.floor("H").dt.time
    hourly_df = df.groupby(
        ["Date", "Time", "Unit", "ToD_Slot"], as_index=False
    ).agg({
        "Consumption_value": "sum",
        "Generation_value": "sum",
        "Surplus_Generation": "sum",
        "Surplus_Demand": "sum"
    })
    hourly_df["Generation_value"] = (
        hourly_df["Generation_value"] + hourly_df["Surplus_Generation"]
    )
    hourly_df = hourly_df.drop(columns=["Surplus_Generation", "Surplus_Demand"])
    validate_nonempty(hourly_df, context="Hourly aggregated data")
    hourly_df.to_excel(output_file, index=False)
    print(f"✅ Hourly aggregated file created: {output_file}")

def format_date_in_csv(input_csv, output_csv):
    import os
    if not os.path.exists(input_csv):
        raise FileNotFoundError(f"File not found: {input_csv}")
    df = pd.read_csv(input_csv)
    validate_columns(df, ["Date"], context="CSV date formatting")
    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
    df.to_csv(output_csv, index=False)
    print(f"✅ Date formatted and saved to {output_csv}")

if __name__ == "__main__":
    # Step 1: Merge inverter data
    base_folder = "Inverter Dump Aug 2025"
    merged_file = "Generation_Data_Aug_Merged.xlsx"
    merge_inverter_data(base_folder, merged_file)
    # Generation data will also be saved to Generation_Data_Aug.xlsx after merging


    # Step 2: Aggregate to 15-minute intervals
    aggregate_15min(merged_file)

    # Step 3: Split Date and Time
    split_date_time(merged_file)

    # Step 4: Merge Generation and Consumption
    consumption_file = "consumption_consolidated_aug.xlsx"
    output_file = "Consumption_Generation_Aug25.xlsx"
    merge_generation_consumption(merged_file, consumption_file, output_file)

    # Step 5: Aggregate to hourly
    hourly_output_file = "Consumption_Generation_Aug25_hourly.xlsx"
    aggregate_hourly(output_file, hourly_output_file)

    # # Step 6: Format date in CSV
    # input_csv = r"CSV/CSV_AUG/Gen_Con_hourly_Aug25_hourly.csv"
    # output_csv = r"CSV/CSV_AUG/Gen_Con_hourly_Aug25_hourly_v2.csv"
    # format_date_in_csv(input_csv, output_csv)
