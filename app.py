import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

MYSQL_USER = "root"
MYSQL_PASSWORD = "venkat"
MYSQL_HOST = "localhost"
MYSQL_DATABASE = "cdta_db"

TRAFFIC_STOPS_TABLE = "traffic_stops"
FLAGGED_VEHICLES_TABLE = "flagged_vehicles"

@st.cache_resource
def get_db_connection():
    try:
        db_connection_str = (
            f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}"
        )
        engine = create_engine(db_connection_str)
        return engine
    except Exception as e:
        st.error(f"Error connecting to the database: {e}")
        st.stop()

def fetch_data(query, params=None):
    engine = get_db_connection()
    try:
        with engine.connect() as connection:
            if params is not None and not isinstance(params, (tuple, dict)):
                params = tuple(params)
            df = pd.read_sql(query, connection, params=params)
        return df
    except Exception as e:
        st.error(f"Error fetching data with query: {query}. Reason: {e}")
        return pd.DataFrame()

def execute_query(query, params=None):
    engine = get_db_connection()
    try:
        with engine.connect() as connection:
            if params is not None and not isinstance(params, (tuple, dict)):
                params = tuple(params)
            connection.execute(text(query), params)
            connection.commit()
    except Exception as e:
        st.error(f"Error executing query: {query}. Reason: {e}")


INSIGHTS = {
    "Top 10 Drug-Related Vehicles": f"""
        SELECT vehicle_number, COUNT(*) as stop_count
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE drugs_related_stop = TRUE AND vehicle_number != 'Unknown'
        GROUP BY vehicle_number
        ORDER BY stop_count DESC
        LIMIT 10;
    """,
    "Most Frequently Searched Vehicles": f"""
        SELECT vehicle_number, COUNT(*) as search_count
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE search_conducted = TRUE AND vehicle_number != 'Unknown'
        GROUP BY vehicle_number
        ORDER BY search_count DESC
        LIMIT 10;
    """,
    "Driver Age Group with Highest Arrest Rate": f"""
        SELECT
            CASE
                WHEN driver_age BETWEEN 15 AND 20 THEN '15-20'
                WHEN driver_age BETWEEN 21 AND 25 THEN '21-25'
                WHEN driver_age BETWEEN 26 AND 35 THEN '26-35'
                WHEN driver_age BETWEEN 36 AND 50 THEN '36-50'
                WHEN driver_age > 50 THEN '50+'
                ELSE 'Unknown'
            END as age_group,
            (SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as arrest_rate
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE driver_age > 0
        GROUP BY age_group
        ORDER BY arrest_rate DESC;
    """,
    "Gender Distribution of Drivers Stopped by Country": f"""
        SELECT country_name, driver_gender, COUNT(*) as stop_count
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE country_name != 'Unknown' AND driver_gender != 'Unknown'
        GROUP BY country_name, driver_gender
        ORDER BY country_name, driver_gender;
    """,
    "Race and Gender Combination with Highest Search Rate": f"""
        SELECT
            driver_race,
            driver_gender,
            (SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as search_rate
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE driver_race != 'Unknown' AND driver_gender != 'Unknown'
        GROUP BY driver_race, driver_gender
        ORDER BY search_rate DESC
        LIMIT 10;
    """,
    "Time of Day with Most Traffic Stops": f"""
        SELECT
            HOUR(stop_time) AS hour_of_day,
            COUNT(*) AS stop_count
        FROM {TRAFFIC_STOPS_TABLE}
        GROUP BY hour_of_day
        ORDER BY stop_count DESC;
    """,
    "Average Stop Duration for Different Violations": f"""
        SELECT violation, AVG(
            CASE stop_duration
                WHEN '0-15 Min' THEN 7.5
                WHEN '16-30 Min' THEN 23
                WHEN '30+ Min' THEN 45
                ELSE 0
            END
        ) as average_duration_minutes
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE violation != 'Unknown'
        GROUP BY violation
        ORDER BY average_duration_minutes DESC;
    """,
    "Night Stops More Likely to Lead to Arrests?": f"""
        SELECT
            CASE
                WHEN HOUR(stop_time) >= 20 OR HOUR(stop_time) < 6
                THEN 'Night'
                ELSE 'Day'
            END as time_of_day_category,
            (SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as arrest_rate
        FROM {TRAFFIC_STOPS_TABLE}
        GROUP BY time_of_day_category
        ORDER BY arrest_rate DESC;
    """,
    "Violations Most Associated with Searches or Arrests": f"""
        SELECT
            violation,
            (SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as search_rate,
            (SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as arrest_rate
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE violation != 'Unknown'
        GROUP BY violation
        ORDER BY search_rate DESC, arrest_rate DESC;
    """,
    "Violations Most Common Among Younger Drivers (<25)": f"""
        SELECT violation, COUNT(*) as stop_count
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE driver_age > 0 AND driver_age < 25 AND violation != 'Unknown'
        GROUP BY violation
        ORDER BY stop_count DESC
        LIMIT 10;
    """,
    "Violation That Rarely Results in Search or Arrest": f"""
        SELECT
            violation,
            (SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as search_rate,
            (SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as arrest_rate
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE violation != 'Unknown'
        GROUP BY violation
        HAVING search_rate < 5 AND arrest_rate < 5
        ORDER BY search_rate ASC, arrest_rate ASC
        LIMIT 5;
    """,
    "Countries with Highest Rate of Drug-Related Stops": f"""
        SELECT country_name,
               (SUM(CASE WHEN drugs_related_stop = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as drug_related_stop_rate
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE country_name != 'Unknown'
        GROUP BY country_name
        ORDER BY drug_related_stop_rate DESC
        LIMIT 10;
    """,
    "Arrest Rate by Country and Violation": f"""
        SELECT country_name, violation,
               (SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as arrest_rate
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE country_name != 'Unknown' AND violation != 'Unknown'
        GROUP BY country_name, violation
        HAVING COUNT(*) > 10
        ORDER BY country_name, arrest_rate DESC;
    """,
    "Country with Most Stops with Search Conducted": f"""
        SELECT country_name, COUNT(*) as search_conducted_stops_count
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE search_conducted = TRUE AND country_name != 'Unknown'
        GROUP BY country_name
        ORDER BY search_conducted_stops_count DESC
        LIMIT 5;
    """,
    "Yearly Breakdown of Stops and Arrests by Country": f"""
        SELECT
            YEAR(stop_date) AS stop_year,
            country_name,
            COUNT(*) AS total_stops,
            SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests,
            (SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS arrest_rate_percentage
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE country_name != 'Unknown' AND stop_date IS NOT NULL
        GROUP BY stop_year, country_name
        ORDER BY stop_year, country_name;
    """,
    "Driver Violation Trends Based on Age and Race": f"""
        SELECT
            ts.driver_race,
            ts.driver_age,
            ts.violation,
            COUNT(*) AS violation_count
        FROM {TRAFFIC_STOPS_TABLE} AS ts
        WHERE ts.driver_race != 'Unknown' AND ts.violation != 'Unknown' AND ts.driver_age > 0
        GROUP BY ts.driver_race, ts.driver_age, ts.violation
        ORDER BY ts.driver_race, ts.driver_age, violation_count DESC
        LIMIT 100;
    """,
    "Time Period Analysis of Stops (Year, Month, Hour)": f"""
        SELECT
            YEAR(stop_date) AS stop_year,
            MONTH(stop_date) AS stop_month,
            HOUR(stop_time) AS stop_hour,
            COUNT(*) AS number_of_stops
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE stop_date IS NOT NULL AND stop_time IS NOT NULL
        GROUP BY stop_year, stop_month, stop_hour
        ORDER BY stop_year, stop_month, stop_hour;
    """,
    "Violations with High Search and Arrest Rates": f"""
        WITH ViolationStats AS (
            SELECT
                violation,
                COUNT(*) AS total_stops,
                SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) AS total_searches,
                SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests
            FROM {TRAFFIC_STOPS_TABLE}
            WHERE violation != 'Unknown'
            GROUP BY violation
        )
        SELECT
            violation,
            total_stops,
            total_searches,
            total_arrests,
            (total_searches * 100.0 / total_stops) AS search_rate_percentage,
            (total_arrests * 100.0 / total_stops) AS arrest_rate_percentage
        FROM ViolationStats
        WHERE total_stops > 50
        ORDER BY search_rate_percentage DESC, arrest_rate_percentage DESC
        LIMIT 10;
    """,
    "Driver Demographics by Country (Age, Gender, and Race)": f"""
        SELECT
            country_name,
            driver_gender,
            driver_race,
            COUNT(*) AS total_stops,
            AVG(driver_age) AS average_driver_age
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE country_name != 'Unknown' AND driver_gender != 'Unknown' AND driver_race != 'Unknown' AND driver_age > 0
        GROUP BY country_name, driver_gender, driver_race
        ORDER BY country_name, total_stops DESC
        LIMIT 100;
    """,
    "Top 5 Violations with Highest Arrest Rates": f"""
        SELECT
            violation,
            (SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as arrest_rate
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE violation != 'Unknown'
        GROUP BY violation
        ORDER BY arrest_rate DESC
        LIMIT 5;
    """
}


st.set_page_config(layout="wide", page_title="SecureCheck Police Post Logs")

st.title("🚓 SecureCheck: Police Post Logs Dashboard")
st.markdown("---")

st.sidebar.header("Navigation")
page = st.sidebar.radio("Go to", ["Dashboard Overview", "Search Logs", "Analytics & Reports", "Flagged Vehicles", "Automated SQL Queries"])
st.sidebar.markdown("---")
st.sidebar.info("This dashboard provides real-time insights into police traffic stop data.")

if page == "Dashboard Overview":
    st.header("Dashboard Overview: Recent Activity")
    recent_logs_query = f"SELECT * FROM {TRAFFIC_STOPS_TABLE} ORDER BY stop_date DESC, stop_time DESC LIMIT 20;"
    recent_df = fetch_data(recent_logs_query)

    if not recent_df.empty:
        st.dataframe(recent_df, use_container_width=True)
    else:
        st.info("No recent traffic stop data available. Check database connection and data.")

    st.markdown("---")

    st.header("Key Statistics")
    col1, col2, col3 = st.columns(3)

    total_stops_df = fetch_data(f"SELECT COUNT(*) FROM {TRAFFIC_STOPS_TABLE};")
    total_stops = total_stops_df.iloc[0,0] if not total_stops_df.empty else 0
    col1.metric("Total Stops Recorded", total_stops)

    total_arrests_df = fetch_data(f"SELECT COUNT(*) FROM {TRAFFIC_STOPS_TABLE} WHERE is_arrested = TRUE;")
    total_arrests = total_arrests_df.iloc[0,0] if not total_arrests_df.empty else 0
    col2.metric("Total Arrests", total_arrests)

    total_searches_df = fetch_data(f"SELECT COUNT(*) FROM {TRAFFIC_STOPS_TABLE} WHERE search_conducted = TRUE;")
    total_searches = total_searches_df.iloc[0,0] if not total_searches_df.empty else 0
    col3.metric("Total Searches Conducted", total_searches)

    st.markdown("---")

    st.header("Interactive Data Visualization")

    violation_counts = fetch_data(f"SELECT violation, COUNT(*) as count FROM {TRAFFIC_STOPS_TABLE} WHERE violation != 'Unknown' GROUP BY violation ORDER BY count DESC;")
    if not violation_counts.empty:
        st.subheader("Stops by Violation Type")
        st.bar_chart(violation_counts.set_index('violation'))
    else:
        st.info("No violation data to display. Check database data.")

    country_counts = fetch_data(f"SELECT country_name, COUNT(*) as count FROM {TRAFFIC_STOPS_TABLE} WHERE country_name != 'Unknown' GROUP BY country_name ORDER BY count DESC;")
    if not country_counts.empty:
        st.subheader("Stops by Country")
        st.bar_chart(country_counts.set_index('country_name'))
    else:
        st.info("No country data to display. Check database data.")


elif page == "Search Logs":
    st.header("🔍 Search Traffic Stop Logs")
    st.write("Filter and search through historical traffic stop records.")

    col1, col2, col3 = st.columns(3)
    
    all_countries = ["All"] + fetch_data(f"SELECT DISTINCT country_name FROM {TRAFFIC_STOPS_TABLE} WHERE country_name != 'Unknown' ORDER BY country_name;").iloc[:, 0].tolist()
    selected_country = col1.selectbox("Filter by Country", all_countries)

    all_genders = ["All"] + fetch_data(f"SELECT DISTINCT driver_gender FROM {TRAFFIC_STOPS_TABLE} WHERE driver_gender != 'Unknown' ORDER BY driver_gender;").iloc[:, 0].tolist()
    selected_gender = col2.selectbox("Filter by Driver Gender", all_genders)

    all_violations = ["All"] + fetch_data(f"SELECT DISTINCT violation FROM {TRAFFIC_STOPS_TABLE} WHERE violation != 'Unknown' ORDER BY violation;").iloc[:, 0].tolist()
    selected_violation = col3.selectbox("Filter by Violation Type", all_violations)

    search_conducted_filter = st.checkbox("Only show stops with search conducted")
    is_arrested_filter = st.checkbox("Only show stops that resulted in arrest")
    
    min_age, max_age = st.slider("Filter by Driver Age", 15, 90, (15, 90))

    vehicle_number_search = st.text_input("Search by Vehicle Number (partial match)", "")

    query_parts = [f"SELECT * FROM {TRAFFIC_STOPS_TABLE} WHERE 1=1"]
    params_list = []

    if selected_country != "All":
        query_parts.append(f"AND country_name = %s")
        params_list.append(selected_country)
    if selected_gender != "All":
        query_parts.append(f"AND driver_gender = %s")
        params_list.append(selected_gender)
    if selected_violation != "All":
        query_parts.append(f"AND violation = %s")
        params_list.append(selected_violation)
    if search_conducted_filter:
        query_parts.append("AND search_conducted = TRUE")
    if is_arrested_filter:
        query_parts.append("AND is_arrested = TRUE")
    
    query_parts.append(f"AND driver_age BETWEEN %s AND %s")
    params_list.append(min_age)
    params_list.append(max_age)

    if vehicle_number_search:
        query_parts.append(f"AND vehicle_number LIKE %s")
        params_list.append(f"%{vehicle_number_search}%")

    search_query = " ".join(query_parts) + f" ORDER BY stop_date DESC, stop_time DESC LIMIT 1000;"

    if st.button("Apply Filters and Search"):
        st.markdown("---")
        st.subheader("Search Results")
        search_results_df = fetch_data(search_query, params_list)
        if not search_results_df.empty:
            st.dataframe(search_results_df, use_container_width=True)
            st.success(f"Found {len(search_results_df)} matching records.")
        else:
            st.info("No records found matching your criteria.")

elif page == "Analytics & Reports":
    st.header("📈 Analytics & Reports")
    st.write("Explore various statistical reports and trends from the traffic stop data.")

    selected_query_name = st.selectbox("Select an Insightful Query", list(INSIGHTS.keys()))
    query_to_run = INSIGHTS[selected_query_name]
    
    # Removed this line: st.code(query_to_run, language='sql')

    if st.button(f"Run {selected_query_name} Report"):
        st.markdown("---")
        st.subheader(f"Results for: {selected_query_name}")
        report_df = fetch_data(query_to_run)
        if not report_df.empty:
            st.dataframe(report_df, use_container_width=True)
            if 'count' in report_df.columns:
                st.bar_chart(report_df.set_index(report_df.columns[0]))
            elif 'arrest_rate' in report_df.columns:
                st.bar_chart(report_df.set_index(report_df.columns[0]))
            elif 'stop_count' in report_df.columns:
                st.bar_chart(report_df.set_index(report_df.columns[0]))
            elif 'search_rate_percentage' in report_df.columns:
                st.bar_chart(report_df.set_index('violation')[['search_rate_percentage', 'arrest_rate_percentage']])
            elif 'number_of_stops' in report_df.columns and ('stop_year' in report_df.columns or 'stop_month' in report_df.columns or 'stop_hour' in report_df.columns):
                if 'stop_year' in report_df.columns and 'stop_month' in report_df.columns and 'stop_hour' in report_df.columns:
                    report_df['time_point'] = report_df['stop_year'].astype(str) + '-' + report_df['stop_month'].astype(str).str.zfill(2) + '-' + report_df['stop_hour'].astype(str).str.zfill(2)
                    st.line_chart(report_df.set_index('time_point')['number_of_stops'])
                else:
                    st.line_chart(report_df.set_index(report_df.columns[0])['number_of_stops'])
        else:
            st.info("No data found for this report.")

elif page == "Flagged Vehicles":
    st.header("🚨 Flagged Vehicles for Review")
    st.write("Vehicles automatically flagged by the detection system for further review.")

    status_filter = st.radio("Show Flags:", ["Active (Unresolved)", "Resolved", "All"])

    query = f"SELECT * FROM {FLAGGED_VEHICLES_TABLE}"
    params = []
    if status_filter == "Active (Unresolved)":
        query += " WHERE resolved = FALSE"
    elif status_filter == "Resolved":
        query += " WHERE resolved = TRUE"
    query += " ORDER BY flag_timestamp DESC"

    flagged_df = fetch_data(query, params)

    if not flagged_df.empty:
        st.dataframe(flagged_df, use_container_width=True)

        st.subheader("Resolve Flag")
        flag_id_to_resolve = st.number_input("Enter Flag ID to Mark as Resolved", min_value=1, format="%d")
        if st.button("Mark as Resolved"):
            if flag_id_to_resolve:
                update_query = f"""
                UPDATE {FLAGGED_VEHICLES_TABLE}
                SET resolved = TRUE
                WHERE flag_id = %s;
                """
                execute_query(update_query, (flag_id_to_resolve,))
                st.success(f"Flag ID {flag_id_to_resolve} marked as resolved.")
                st.rerun()
            else:
                st.warning("Please enter a valid Flag ID.")
    else:
        st.info("No flagged vehicles to display based on the current filter.")


elif page == "Automated SQL Queries":
    st.header("🤖 Automated SQL Query Executor")
    st.write("This section allows you to run custom SQL queries directly against the database.")
    st.warning("Only use SELECT queries. Malicious or non-SELECT queries will not be executed directly.")

    custom_query = st.text_area("Enter your custom SQL SELECT query:", height=150, value=f"SELECT * FROM {TRAFFIC_STOPS_TABLE} LIMIT 10;")

    if st.button("Execute Custom Query"):
        if custom_query.strip().upper().startswith("SELECT"):
            try:
                custom_df = fetch_data(custom_query)
                if not custom_df.empty:
                    st.success("Query executed successfully!")
                    st.dataframe(custom_df, use_container_width=True)
                else:
                    st.info("Query returned no results.")
            except Exception as e:
                st.error(f"Error executing query: {e}")
        else:
            st.error("Only SELECT queries are allowed for direct execution here.")
