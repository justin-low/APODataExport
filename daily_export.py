import datetime as dt
import pymssql

from src.models.connector import SqlServerConnection, ConnectionType

from id_finder import IdLookUp
import apo_extract_script

# Input Variables
# Date Target
date_target = dt.datetime(year=2025, month=1, day=18)  # Date to export
split = False # Split
# -------------------------------------------------------------------------------------------------------------------- #
# Prep Variables
_start = date_target - dt.timedelta(hours=1)
_end = date_target + dt.timedelta(days=1, hours=1)


# Database Variables
# Initialize sql connection
conn_config = SqlServerConnection(ConnectionType.NewSkies)
connection = pymssql.connect(f"{conn_config.uri + ':' + conn_config.port}", conn_config.user,
                                           conn_config.pwd, conn_config.database)

cursor = connection.cursor(as_dict=False)

# Look Up Treatment ID Bound ----------------------------------------------------------------------------------------- #
table_name = "ANAJQDW01.AnalyticsDW.Treatment"
id_column = "TreatmentID"
time_column = "LoggedUTC"
look_up_object = IdLookUp()

# Get Start id and corresponding startid datetime
print("extracting for start time: ", _start)
logged_start_id, logged_start_datetime = look_up_object.find_id_based_on_time(
    cursor,
    _start,
    table_name,
    id_column,
    time_column, "before", None, None
)
start_id = logged_start_id # Forward Catch
print("extracting for end time: ", _end)
logged_end_id, logged_end_datetime = look_up_object.find_id_based_on_time(
    cursor,
    _end,
    table_name,
    id_column,
    time_column, "after", start_id, None
)

id_look_up_report = {
    "date": date_target.strftime('%Y-%m-%d'),
    "start": date_target,  # Start of the day
    "end":  date_target + dt.timedelta(days=1),  # End of the day
    "logged_start_id": logged_start_id,  # Start ID
    "logged_start_datetime": logged_start_datetime,  # Logged time for start ID
    "logged_end_id": logged_end_id,  # End ID
    "logged_end_datetime": logged_end_datetime  # Logged time for end ID
}

print(id_look_up_report)

# Extract tables ----------------------------------------------------------------------------------------------------- #

# Flatten and inverse filter relationship input
table_filter_lookup = apo_extract_script.cross_join_inverse(apo_extract_script.filter_relationship)
for i in apo_extract_script.treatment_files:
    with open(f"./src/RawSQLQueries/{i}.sql") as file:
        sql_query = file.read()

    # Get Filter key
    filter_key = table_filter_lookup[i]  # id lookup key
    date_start = id_look_up_report["start"]  # Final Date Range Filter
    date_end = id_look_up_report["end"]  # Final Date Range Filter
    id_start = id_look_up_report["logged_start_id"]  # SQL Indexed Filter
    id_end = id_look_up_report["logged_end_id"]  # SQL Indexed Filter
    filter_column = apo_extract_script.column_relationship[filter_key]  # Final  Date Range Filter Column

    print(f'pulling {i} for {date_start.strftime("%Y-%m-%d")}')

    batch_size = 100000
    if not split:
        res_df_first_half = apo_extract_script.retrieve_and_process_data(cursor, sql_query, id_start, id_end, batch_size,
                                                      apo_extract_script.data_types[i], filter_column, date_start, date_end)
        apo_extract_script.save_and_zip_data(res_df_first_half, i, date_start, date_end, suffix="")
    else:
        apo_extract_script.process_data_with_split(cursor, sql_query, id_start, id_end, batch_size,
                                apo_extract_script.data_types[i], filter_column, date_start, date_end, i)