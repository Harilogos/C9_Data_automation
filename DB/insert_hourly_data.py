import pandas as pd
from DB.db_connection import get_connection

# --- CONFIGURATION ---
EXCEL_PATH = "Final Files/C9_August_20251114_111847/Consumption_Generation_Aug25_hourly.xlsx"

TABLE_NAME = "hourly_gen_con2_v2"

def insert_hourly_data(
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
            row["Date"],                # date
            row["Time"],                # time
            row["Unit"],                # unit (name, as in Excel)
            row["ToD_Slot"],            # tod_slot
            row["Consumption_value"],   # consumption
            row["Generation_value"]     # supplied_generation
        )
        records.append(record)
        unique_keys.append((row["Date"], row["Time"], row["Unit"]))

    if not records:
        print("No valid records to insert.")
        return

    # 3. Check for existing records in DB and filter out duplicates
    conn = get_connection()
    cur = conn.cursor()
    unique_keys_set = set(unique_keys)
    if unique_keys_set:
        key_tuples = ",".join(
            cur.mogrify("(%s, %s, %s)", (d, t, u)).decode("utf-8")
            for d, t, u in unique_keys_set
        )
        check_sql = f"""
            SELECT date, time, unit
            FROM {table_name}
            WHERE (date, time, unit) IN ({key_tuples})
        """
        cur.execute(check_sql)
        existing = set(cur.fetchall())
    else:
        existing = set()

    filtered_records = [
        rec for rec, key in zip(records, unique_keys) if (key[0], key[1], key[2]) not in existing
    ]

    if not filtered_records:
        print("All records already exist in the database. No new records to insert.")
        cur.close()
        conn.close()
        return

    # 4. Insert only new records
    insert_sql = f"""
        INSERT INTO {table_name}
        (date, time, unit, tod_slot, consumption, supplied_generation)
        VALUES (%s, %s, %s, %s, %s, %s)
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
    insert_hourly_data()
