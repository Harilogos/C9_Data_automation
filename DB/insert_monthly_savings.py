import pandas as pd
from DB.db_connection import get_connection

# --- CONFIGURATION ---
EXCEL_PATH = "Final Files/C9_August_20251114_111847/monthly_saving.xlsx"

TABLE_NAME = "monthly_savings_v2"

def insert_monthly_savings(
    excel_path=EXCEL_PATH,
    table_name=TABLE_NAME,
    verbose=True
):
    # 1. Read Excel
    df = pd.read_excel(excel_path)

    # 2. Prepare data for insertion
    records = []
    unique_keys = []
    for _, row in df.iterrows():
        record = (
            row["Month"],                                 # month
            row["Unit"],                                  # unit (full string)
            row["Consumption_value"],                     # consumption
            row["grid_cost"],                             # grid_cost
            row["actual_cost_with_banking"],              # actual_cost_with_banking
            row["savings_with_banking"],                  # savings_with_banking
            row["savings_pct_with_banking"],              # savings_pct_with_banking
            row["actual_cost_without_banking"],           # actual_cost_without_banking
            row["savings_without_banking"],               # savings_without_banking
            row["savings_pct_without_banking"]            # savings_pct_without_banking
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
        (month, unit, consumption, grid_cost, actual_cost_with_banking, savings_with_banking, savings_pct_with_banking, actual_cost_without_banking, savings_without_banking, savings_pct_without_banking)
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
    insert_monthly_savings()
