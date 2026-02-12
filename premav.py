import numpy as np
import pandas as pd
import mysql.connector

def clean_premav_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans data, fills demographics, and sets a sequential record_id.
    """
    pd.set_option('future.no_silent_downcasting', True)
    df = data.copy()

    # 1. Fill demographic/bilingual columns per the original record_id
    cols_to_fill = [
        'redcap_event_name', 'redcap_repeat_instrument', 'redcap_repeat_instance', 
        'participant_id', 'enrollment_date', 'participant_initials', 'date_of_birth', 
        'years', 'gender', 'school_completed', 'phone_number', 'demographics_complete'
    ]
    existing = [c for c in cols_to_fill if c in df.columns]
    df[existing] = (
        df.groupby("record_id")[existing]
          .transform(lambda x: x.ffill().bfill())
          .infer_objects(copy=False)
    )

    # 2. Cleanup and Row Indexing
    df = df.dropna(subset=['participant_id']).reset_index(drop=True)
    
    # Create the sequence range
    df["id_sequence"] = range(1, len(df) + 1)
    
    # 3. Drop existing 'record_id' and insert the new range at Index 0
    # This ensures it NEVER gets dropped by the :256 slice
    if 'record_id' in df.columns:
        df = df.drop(columns=['record_id'])
    df.insert(0, 'record_id', df["id_sequence"])
    df = df.drop(columns=['id_sequence'])

    # 4. Limit to 256 columns (record_id is now safely at index 0)
    df = df.iloc[:, :256]

    # 5. Status Logic
    conditions = [
        df['participant_id'].str.contains('MR|MI', na=False),
        df['participant_id'].str.contains('MC', na=False),
        df['participant_id'].str.contains('MO', na=False)
    ]
    df['status'] = np.select(conditions, ['Recovered', 'Contact', 'Control'], default='Unknown')
    df = df[df['clinical_information_review_systems_and_medical_history_complete'] == 'Complete']
    df.to_excel('dataset_123.xlsx', index=False)
    return df

def save_to_mysql(df: pd.DataFrame, table_name: str, unique_column: str):
    """
    Saves data to MySQL. If schema changes, it recreates the table.
    """
    DB_CONFIG = {
        "host": "127.0.0.1", "port": 3307,
        "user": "root", "password": "yourpassword",
        "database": "rids"
    }

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Step 1: Force Table Creation/Alignment
        # If record_id is missing from the DB but in DF, we drop and recreate
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        if cursor.fetchone():
            cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
            db_cols = [row[0] for row in cursor.fetchall()]
            if unique_column not in db_cols:
                print(f"🔄 Column {unique_column} missing in DB. Recreating table...")
                cursor.execute(f"DROP TABLE `{table_name}`")

        # Step 2: Create Schema
        cols_sql = []
        for col, dtype in df.dtypes.items():
            sql_type = "INT" if "int" in str(dtype).lower() else "FLOAT" if "float" in str(dtype).lower() else "TEXT"
            pk = " PRIMARY KEY" if col == unique_column else ""
            cols_sql.append(f"`{col}` {sql_type}{pk}")

        cursor.execute(f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(cols_sql)})")

        # Step 3: Sync Data
        df_sql = df.replace({np.nan: None})
        columns_str = ", ".join(f"`{c}`" for c in df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))
        
        # REPLACE INTO ensures our range-IDs (1, 2, 3...) refresh their data
        sql = f"REPLACE INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"
        cursor.executemany(sql, [tuple(x) for x in df_sql.to_numpy()])
        
        conn.commit()
        print(f"✅ Sync Successful: {len(df)} rows updated.")

    except mysql.connector.Error as err:
        print(f"❌ MySQL Error: {err}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()