import pandas as pd
from DB.db_connection import get_connection

# --- CONFIGURATION ---
EXCEL_PATH = "Final Files/C9_August_20251114_111847/Consumption_Generation_Aug25.xlsx"
SHEET_NAME = "banking_settlement"

TABLE_NAME = "monthly_banking_settlement_data_v2"

def insert_monthly_banking_settlement(
    excel_path=EXCEL_PATH,
    sheet_name=SHEET_NAME,
    table_name=TABLE_NAME,
    verbose=True
):
    # 1. Read Excel
    df = pd.read_excel(excel_path, sheet_name=sheet_name)

    # 2. Prepare data for insertion
    records = []
    unique_keys = []
    for _, row in df.iterrows():
        record = (
            row["Month"],                                 # month
            row["Unit"],                                  # unit (full string, e.g., "BELLANDUR (S11HT-124)")
            row["Consumption_value"],                     # consumption
            row["Generation_value"],                      # supplied_generation
            row["Surplus_Generation"],                    # surplus_generation
            row["Surplus_Demand"],                        # surplus_demand
            row["Matched_Settlement"],                    # matched_settlement
            row["Settlement_with_Banking"],               # settlement_with_banking
            row["Surplus_Generation_After_Banking"],      # surplus_generation_after_banking
            row["Surplus_Demand_After_Banking"]           # surplus_demand_after_banking
        )
        records.append(record)
        unique_keys.append((row["Month"], row["Unit"]))

    if not records:
        print("No valid records to insert.")
        return

    # 3. Check for existing records in DB and filter out duplicates
    conn = get_connection()
    cur = conn.cursor()
    unique_keys_set = set(unique_keys)
    if unique_keys_set:
        key_tuples = ",".join(
            cur.mogrify("(%s, %s)", (m, u)).decode("utf-8")
            for m, u in unique_keys_set
        )
        check_sql = f"""
            SELECT month, unit
            FROM {table_name}
            WHERE (month, unit) IN ({key_tuples})
        """
        cur.execute(check_sql)
        existing = set(cur.fetchall())
    else:
        existing = set()

    filtered_records = [
        rec for rec, key in zip(records, unique_keys) if (key[0], key[1]) not in existing
    ]

    if not filtered_records:
        print("All records already exist in the database. No new records to insert.")
        cur.close()
        conn.close()
        return

    # 4. Insert only new records
    insert_sql = f"""
        INSERT INTO {table_name}
        (month, unit, consumption, supplied_generation, surplus_generation, surplus_demand, matched_settlement, settlement_with_banking, surplus_generation_after_banking, surplus_demand_after_banking)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    try:
        cur.executemany(insert_sql, filtered_records)
        conn.commit()
        print(f"Inserted {cur.rowcount} new rows into {table_name}. Skipped {len(records) - len(filtered_records)} duplicate rows.")
    except Exception as e:
        print("Error during insertion:", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    insert_monthly_banking_settlement()
