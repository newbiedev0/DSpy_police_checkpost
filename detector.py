
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

MYSQL_USER = "root"
MYSQL_PASSWORD = "venkat"
MYSQL_HOST = "localhost"
MYSQL_DATABASE = "cdta_db"

TRAFFIC_STOPS_TABLE = "traffic_stops"
FLAGGED_VEHICLES_TABLE = "flagged_vehicles"

def get_db_connection():
    db_connection_str = (
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}"
    )
    engine = create_engine(db_connection_str)
    return engine

def fetch_data(query, params=None):
    engine = get_db_connection()
    with engine.connect() as connection:
        df = pd.read_sql(text(query), connection, params=params)
    return df

def execute_query(query, params=None):
    engine = get_db_connection()
    with engine.connect() as connection:
        connection.execute(text(query), params)
        connection.commit()

def create_flagged_vehicles_table():
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {FLAGGED_VEHICLES_TABLE} (
        flag_id INT AUTO_INCREMENT PRIMARY KEY,
        vehicle_number VARCHAR(255) NOT NULL,
        flag_reason TEXT,
        flag_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        resolved BOOLEAN DEFAULT FALSE,
        INDEX(vehicle_number)
    );
    """
    try:
        execute_query(create_table_query)
    except Exception as e:
        pass 

def run_detection_rules():
    create_flagged_vehicles_table()

    last_flag_time_query = f"SELECT MAX(flag_timestamp) FROM {FLAGGED_VEHICLES_TABLE};"
    last_flag_time_df = fetch_data(last_flag_time_query)
    last_flag_time = last_flag_time_df.iloc[0,0] if not last_flag_time_df.empty and last_flag_time_df.iloc[0,0] is not None else (datetime.now() - timedelta(days=365))

    speeding_query = f"""
    SELECT
        ts.vehicle_number,
        COUNT(*) as violation_count
    FROM {TRAFFIC_STOPS_TABLE} ts
    WHERE
        ts.violation = 'Speeding' AND ts.vehicle_number != 'Unknown'
        AND ts.stop_date >= :time_window_30_days
    GROUP BY ts.vehicle_number
    HAVING COUNT(*) > 2;
    """
    time_window_30_days = datetime.now() - timedelta(days=30)
    speeding_flags_df = fetch_data(speeding_query, {'time_window_30_days': time_window_30_days})

    for index, row in speeding_flags_df.iterrows():
        vehicle = row['vehicle_number']
        reason = f"Multiple Speeding Violations ({row['violation_count']} in last 30 days)"
        
        check_existing_flag_query = f"""
        SELECT COUNT(*) FROM {FLAGGED_VEHICLES_TABLE}
        WHERE vehicle_number = :vehicle_num AND flag_reason = :reason_text AND resolved = FALSE;
        """
        existing_flags_df = fetch_data(check_existing_flag_query, {'vehicle_num': vehicle, 'reason_text': reason})
        if existing_flags_df.iloc[0,0] == 0:
            insert_flag_query = f"""
            INSERT INTO {FLAGGED_VEHICLES_TABLE} (vehicle_number, flag_reason)
            VALUES (:vehicle_num, :reason_text);
            """
            execute_query(insert_flag_query, {'vehicle_num': vehicle, 'reason_text': reason})
        else:
            pass 

    drug_stop_query = f"""
    SELECT DISTINCT ts.vehicle_number
    FROM {TRAFFIC_STOPS_TABLE} ts
    WHERE ts.drugs_related_stop = TRUE AND ts.vehicle_number != 'Unknown'
    AND ts.stop_date >= :last_flag_date;
    """
    drug_stop_flags_df = fetch_data(drug_stop_query, {'last_flag_date': last_flag_time.date()})

    for index, row in drug_stop_flags_df.iterrows():
        vehicle = row['vehicle_number']
        reason = "Involved in Drug-Related Stop"

        check_existing_flag_query = f"""
        SELECT COUNT(*) FROM {FLAGGED_VEHICLES_TABLE}
        WHERE vehicle_number = :vehicle_num AND flag_reason = :reason_text AND resolved = FALSE;
        """
        existing_flags_df = fetch_data(check_existing_flag_query, {'vehicle_num': vehicle, 'reason_text': reason})
        if existing_flags_df.iloc[0,0] == 0:
            insert_flag_query = f"""
            INSERT INTO {FLAGGED_VEHICLES_TABLE} (vehicle_number, flag_reason)
            VALUES (:vehicle_num, :reason_text);
            """
            execute_query(insert_flag_query, {'vehicle_num': vehicle, 'reason_text': reason})
        else:
            pass 

    high_arrest_driver_query = f"""
    SELECT
        ts.vehicle_number,
        ts.driver_gender,
        ts.driver_race,
        (SUM(CASE WHEN ts.is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS arrest_rate_percentage
    FROM {TRAFFIC_STOPS_TABLE} ts
    WHERE ts.vehicle_number != 'Unknown' AND ts.driver_gender != 'Unknown' AND ts.driver_race != 'Unknown'
    GROUP BY ts.vehicle_number, ts.driver_gender, ts.driver_race
    HAVING arrest_rate_percentage > 50;
    """
    high_arrest_flags_df = fetch_data(high_arrest_driver_query)

    for index, row in high_arrest_flags_df.iterrows():
        vehicle = row['vehicle_number']
        reason = f"High Arrest Rate Driver (Race: {row['driver_race']}, Gender: {row['driver_gender']})"
        
        check_existing_flag_query = f"""
        SELECT COUNT(*) FROM {FLAGGED_VEHICLES_TABLE}
        WHERE vehicle_number = :vehicle_num AND flag_reason = :reason_text AND resolved = FALSE;
        """
        existing_flags_df = fetch_data(check_existing_flag_query, {'vehicle_num': vehicle, 'reason_text': reason})
        if existing_flags_df.iloc[0,0] == 0:
            insert_flag_query = f"""
            INSERT INTO {FLAGGED_VEHICLES_TABLE} (vehicle_number, flag_reason)
            VALUES (:vehicle_num, :reason_text);
            """
            execute_query(insert_flag_query, {'vehicle_num': vehicle, 'reason_text': reason})
        else:
            pass 

if __name__ == "__main__":
    run_detection_rules()
    
    print("--- Detector Script Finished ---")