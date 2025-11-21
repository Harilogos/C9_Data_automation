import pandas as pd
import json
from DB.db_connection import get_connection

# --- CONFIGURATION ---
EXCEL_PATH = "Final Files/C9_August_20251114_111847/Consumption_Generation_Aug25.xlsx"
SHEET_NAME = "15 mins"
LOCATION_UNITS_PATH = "location_units.json"

TABLE_NAME = "gen_cons_15min_data_v2"

def load_location_unit_map(json_path):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # Map: location name -> unit_id
    return {entry["location"]: entry["unit_id"] for entry in data}

def insert_15min_data(
    excel_path=EXCEL_PATH,
    sheet_name=SHEET_NAME,
    location_units_path=LOCATION_UNITS_PATH,
    table_name=TABLE_NAME,
    verbose=True
):
    # 1. Read Excel
    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    # 2. Load location-unit mapping
    location_map = load_location_unit_map(location_units_path)

    # 3. Prepare data for insertion
    records = []
    unique_keys = []
    for _, row in df.iterrows():
        location_name = row["Unit"]
        unit_code = location_map.get(location_name)
        if not unit_code:
            print(f"Warning: No unit code found for location '{location_name}'. Skipping row.")
            continue
        record = (
            row["Date"],                # reading_date
            row["Time"],                # reading_time
            location_name,              # location (name)
            unit_code,                  # unit (code)
            row["ToD_Slot"],            # tod_slot
            row["Consumption_value"],   # consumption
            row["Generation_value"]     # supplied_generation
        )
        records.append(record)
        unique_keys.append((row["Date"], row["Time"], unit_code))

    if not records:
        print("No valid records to insert.")
        return

    # 4. Check for existing records in DB and filter out duplicates
    conn = get_connection()
    cur = conn.cursor()
    # Prepare a set of unique keys to check
    unique_keys_set = set(unique_keys)
    # Query for existing keys in the DB
    if unique_keys_set:
        # Build WHERE clause for batch check
        key_tuples = ",".join(
            cur.mogrify("(%s, %s, %s)", (d, t, u)).decode("utf-8")
            for d, t, u in unique_keys_set
        )
        check_sql = f"""
            SELECT reading_date, reading_time, unit
            FROM {TABLE_NAME}
            WHERE (reading_date, reading_time, unit) IN ({key_tuples})
        """
        cur.execute(check_sql)
        existing = set(cur.fetchall())
    else:
        existing = set()

    # Filter out records that already exist
    filtered_records = [
        rec for rec, key in zip(records, unique_keys) if (key[0], key[1], key[2]) not in existing
    ]

    if not filtered_records:
        print("All records already exist in the database. No new records to insert.")
        cur.close()
        conn.close()
        return

    # 5. Insert only new records
    insert_sql = f"""
        INSERT INTO {TABLE_NAME}
        (reading_date, reading_time, location, unit, tod_slot, consumption, supplied_generation)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    try:
        cur.executemany(insert_sql, filtered_records)
        conn.commit()
        print(f"Inserted {cur.rowcount} new rows into {TABLE_NAME}. Skipped {len(records) - len(filtered_records)} duplicate rows.")
    except Exception as e:
        print("Error during insertion:", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    insert_15min_data()
