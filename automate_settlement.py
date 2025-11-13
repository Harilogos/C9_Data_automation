import pandas as pd

def calculate_matched_settlement(input_file: str, input_sheet: str, output_sheet: str) -> None:
    df = pd.read_excel(input_file, sheet_name=input_sheet)
    # Calculate matched settlement
    df['Matched_Settlement'] = df[['Generation_value', 'Consumption_value']].min(axis=1)
    # Keep only date (no time)
    df['Date'] = pd.to_datetime(df['Date']).dt.date
    # Write back to Excel
    with pd.ExcelWriter(input_file, mode="a", engine="openpyxl", if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name=output_sheet, index=False)
    print(f"✅ Matched settlement column added and saved to sheet '{output_sheet}'.")
    import gc, time
    gc.collect()
    time.sleep(0.1)

def add_unit_id(input_file: str, input_sheet: str, output_sheet: str) -> None:
    # Create mapping dict
    unit_map = {
        "MALLESWARAM": "C2HT-136",
        "ELECTRONIC CITY": "S13HT-87",
        "KANAKAPURA": "S12HT-99",
        "BELLANDUR": "S11HT-124",
        "SARJAPURA": "S11HT-419",
        "SAHAKAR NAGAR": "C8HT-111",
        "HRBR UNIT": "E8HT-203",
        "WHITEFIELD": "E4HT-355",
        "BELLANDUR CORP. OFFICE": "S11BHT 406",
        "THANISANDRA": "C8HT-135",
        "OLD AIRPORT ROAD": "E6HT209"
    }
    df = pd.read_excel(input_file, sheet_name=input_sheet)
    print("DEBUG: Columns in DataFrame before mapping:", df.columns.tolist())
    # Prefer "Location" if present, else use "Unit"
    if "Location" in df.columns:
        df["Unit"] = df["Location"].str.upper().map(lambda u: f"{u} ({unit_map.get(u, '')})")
    elif "Unit" in df.columns:
        # Only update if not already mapped (i.e., if not already in the format "NAME (ID)")
        def map_unit(u):
            u_upper = str(u).upper()
            if "(" in u_upper and ")" in u_upper:
                return u  # Already mapped
            return f"{u_upper} ({unit_map.get(u_upper, '')})"
        df["Unit"] = df["Unit"].map(map_unit)
    else:
        raise KeyError("Neither 'Location' nor 'Unit' column found in input sheet for unit ID mapping.")
    with pd.ExcelWriter(input_file, mode="a", engine="openpyxl", if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name=output_sheet, index=False)
    print(f"✅ Unit IDs added and saved to sheet '{output_sheet}'")
    import gc, time
    gc.collect()
    time.sleep(0.1)

def monthly_aggregation(input_file: str, input_sheet: str, output_sheet: str) -> None:
    df = pd.read_excel(input_file, sheet_name=input_sheet)
    df["Date"] = pd.to_datetime(df["Date"])
    df["Month"] = df["Date"].dt.to_period("M").astype(str)
    monthly_df = df.groupby(
        ["Month", "Unit"], as_index=False
    ).agg({
        "Consumption_value": "sum",
        "Generation_value": "sum",
        "Surplus_Generation": "sum",
        "Surplus_Demand": "sum",
        "Matched_Settlement": "sum"
    })
    with pd.ExcelWriter(input_file, mode="a", engine="openpyxl", if_sheet_exists="replace") as writer:
        monthly_df.to_excel(writer, sheet_name=output_sheet, index=False)
    print(f"✅ Monthly aggregated data saved to sheet '{output_sheet}'")
    import gc, time
    gc.collect()
    time.sleep(0.1)

def apply_monthly_banking_settlement(input_file: str, input_sheet: str = "monthly", output_sheet: str = "banking_settlement") -> None:
    df = pd.read_excel(input_file, sheet_name=input_sheet)
    df["Month"] = df["Month"]
    priority_units = ["MALLESWARAM", "SAHAKAR NAGAR", "HRBR UNIT", "OLD AIRPORT ROAD"]
    results = []
    for month, month_df in df.groupby("Month"):
        total_gen = month_df["Surplus_Generation"].sum()
        month_df = month_df.copy()
        month_df["Settlement_with_Banking"] = 0.0
        month_df["Surplus_Generation_After_Banking"] = 0.0
        month_df["Surplus_Demand_After_Banking"] = 0.0
        pr_df = month_df[month_df["Unit"].str.contains("|".join(priority_units), case=False, regex=True)]
        other_df = month_df.drop(pr_df.index)
        if not pr_df.empty:
            pr_df["PriorityOrder"] = pr_df["Unit"].apply(
                lambda u: priority_units.index(next(p for p in priority_units if p.lower() in u.lower()))
            )
            pr_df = pr_df.sort_values("PriorityOrder")
        other_df = other_df.sort_values("Surplus_Demand", ascending=False)
        ordered_units = pd.concat([pr_df, other_df])
        for idx, row in ordered_units.iterrows():
            demand = row["Surplus_Demand"]
            if total_gen >= demand * 1.08:
                settlement = demand
                total_gen -= demand * 1.08
            elif total_gen > 0:
                settlement = total_gen * 0.92
                total_gen = 0
            else:
                settlement = 0
            month_df.at[idx, "Settlement_with_Banking"] = settlement
            month_df.at[idx, "Surplus_Generation_After_Banking"] = max(total_gen, 0)
            month_df.at[idx, "Surplus_Demand_After_Banking"] = max(demand - settlement, 0)
        results.append(month_df)
    final_df = pd.concat(results)
    with pd.ExcelWriter(input_file, mode="a", engine="openpyxl", if_sheet_exists="replace") as writer:
        final_df.to_excel(writer, sheet_name=output_sheet, index=False)
    print(f"✅ Banking settlement applied and saved to sheet '{output_sheet}'.")
    import gc, time
    gc.collect()
    time.sleep(0.1)

def calculate_savings_comparison(input_file: str, sheet_name: str, high_grid_rate_per_kwh: float, low_grid_rate_per_kwh: float, renewable_rate_per_kwh: float, output_sheet: str = "monthly_saving") -> None:
    df = pd.read_excel(input_file, sheet_name=sheet_name).copy()
    high_rate_units = [
        "MALLESWARAM (C2HT-136)",
        "HRBR UNIT (E8HT-203)",
        "OLD AIRPORT ROAD (E6HT209)",
        "SAHAKAR NAGAR (C8HT-111)"
    ]
    df["grid_rate"] = df["Unit"].apply(
        lambda u: high_grid_rate_per_kwh if u in high_rate_units else low_grid_rate_per_kwh
    )
    df["grid_cost"] = df["Consumption_value"] * df["grid_rate"]
    # With Banking
    df["grid_consumption_with_banking"] = (
        df["Consumption_value"] - (df["Settlement_with_Banking"] + df["Matched_Settlement"])
    ).clip(lower=0)
    df["actual_cost_with_banking"] = (
        df["grid_consumption_with_banking"] * df["grid_rate"] +
        (df["Settlement_with_Banking"] + df["Matched_Settlement"]) * renewable_rate_per_kwh
    )
    df["savings_with_banking"] = df["grid_cost"] - df["actual_cost_with_banking"]
    df["savings_pct_with_banking"] = (
        df["savings_with_banking"] / df["grid_cost"] * 100
    ).round(2)
    # Without Banking
    df["grid_consumption_without_banking"] = (
        df["Consumption_value"] - df["Matched_Settlement"]
    ).clip(lower=0)
    df["actual_cost_without_banking"] = (
        df["grid_consumption_without_banking"] * df["grid_rate"] +
        df["Matched_Settlement"] * renewable_rate_per_kwh
    )
    df["savings_without_banking"] = df["grid_cost"] - df["actual_cost_without_banking"]
    df["savings_pct_without_banking"] = (
        df["savings_without_banking"] / df["grid_cost"] * 100
    ).round(2)
    output_cols = [
        "Month", "Unit", "Consumption_value",
        "grid_cost",
        "actual_cost_with_banking", "savings_with_banking", "savings_pct_with_banking",
        "actual_cost_without_banking", "savings_without_banking", "savings_pct_without_banking"
    ]
    df = df[output_cols]
    with pd.ExcelWriter(input_file, mode="a", engine="openpyxl", if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name=output_sheet, index=False)
        print("Monthly saving data saved")
    import gc, time
    gc.collect()
    time.sleep(0.1)

def main():
    input_file = "Consumption_Generation_Aug25.xlsx"
    # Step 1: Matched Settlement
    calculate_matched_settlement(input_file, "15 mins", "matched_settlement")
    # Step 2: Add Unit ID
    add_unit_id(input_file, "matched_settlement", "matched_settlement_with_id")
    # Step 3: Monthly Aggregation
    monthly_aggregation(input_file, "matched_settlement_with_id", "monthly")
    # Step 4: Banking Settlement
    apply_monthly_banking_settlement(input_file, "monthly", "banking_settlement")
    # Step 5: Savings Calculation
    calculate_savings_comparison(
        input_file=input_file,
        sheet_name="banking_settlement",
        high_grid_rate_per_kwh=7.20,
        low_grid_rate_per_kwh=5.95,
        renewable_rate_per_kwh=1.0,
        output_sheet="monthly_saving"
    )

if __name__ == "__main__":
    main()
