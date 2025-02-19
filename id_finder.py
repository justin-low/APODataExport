# Internal
import time
import traceback
from typing import Optional

from pymssql import Cursor

from src.models.connector import SqlServerConnection, ConnectionType

import datetime as dt
import pymssql
import polars as pl

# Initialize sql connection
conn_config = SqlServerConnection(ConnectionType.NewSkies)
connection = pymssql.connect(f"{conn_config.uri + ':' + conn_config.port}", conn_config.user,
                                           conn_config.pwd, conn_config.database)


class IdLookUp:
    # Helper functions ---------------------------------------------------------------------------------------------- //
    # Get the min and max of the ID column from the table
    @staticmethod
    def get_min_max_id(target_cursor: Cursor, target_table: str, target_id_col: str):
        query = f"SELECT MIN({target_id_col}), MAX({target_id_col}) FROM {target_table}"
        target_cursor.execute(query)
        min_id, max_id = target_cursor.fetchone()
        print(f"min_id: {min_id}, max_id: {max_id}")
        return min_id, max_id

    @staticmethod
    # Get the time target for a specific ID
    def get_time_by_id(target_cursor: Cursor, target_id: str, target_table: str, target_id_col: str,
                       target_time_col: str):
        query = f"SELECT TOP(1) {target_id_col}, {target_time_col} FROM {target_table} " \
                f"WHERE {target_id_col} >= %s"
        target_cursor.execute(query, str(target_id))
        result = target_cursor.fetchone()
        print(result)
        if result is not None:
            return result[0], result[1]
        else:
            return None

    # Find id based on time
    def find_id_based_on_time(self, target_cursor: Cursor, target_time, target_table, target_id_col, target_time_col,
                              direction="before", start_id: Optional[int] = None, start_step: Optional[int] = None):
        min_id, max_id = self.get_min_max_id(target_cursor, target_table, target_id_col)
        current_id = (min_id + max_id) // 2 if start_id is None else start_id  # Set Initial ID
        closest_id = None
        closest_time = None
        closest_diff = None
        search_step = (max_id - min_id) // 4 if start_step is None else start_step  # Initial search step
        counter = 0

        while min_id <= max_id:
            counter += 1
            print("current_iter: ", counter, "id: ", current_id, " search_step: ", search_step)
            closest_id_result, logged_time = self.get_time_by_id(target_cursor, current_id, target_table, target_id_col,
                                                          target_time_col)
            time.sleep(1)

            if logged_time is None or closest_id_result is None:
                print(f"ID {current_id} does not exist. Adjusting search range...")
                # current_id = closest_id
                if current_id < min_id:
                    current_id = min_id
                elif current_id > max_id:
                    current_id = max_id
                continue

            # Check if logged_time is within the margin of 5 minutes based on the specified direction
            if direction == 'before' and ((target_time - logged_time).total_seconds() < 300) \
                    and ((target_time - logged_time).total_seconds() > 0):
                print(f"Closest ID {closest_id_result} is within 5 minutes after the target time.")
                return closest_id_result, logged_time  # Exit if within margin before target
            elif direction == 'after' and ((logged_time - target_time).total_seconds() < 300) \
                    and ((logged_time - target_time).total_seconds() > 0):
                print(f"Closest ID {closest_id_result} is within 5 minutes before the target time.")
                return closest_id_result, logged_time  # Exit if within margin after target

            # Time difference
            time_diff = abs((logged_time - target_time)).total_seconds()

            # Check if this is the closest match
            if closest_diff is None or time_diff < closest_diff:
                if (direction == 'before' and logged_time <= target_time) \
                        or (direction == 'after' and logged_time >= target_time):
                    closest_diff = time_diff
                    closest_id = closest_id_result
                    closest_time = logged_time

            # Adjust the search direction based on the comparison
            if logged_time < target_time:
                if direction == 'before':
                    closest_id = current_id
                    closest_time = logged_time
                min_id = current_id + 1  # Move search upwards
                current_id = min([current_id + search_step, max_id])
            elif logged_time > target_time:
                if direction == 'after':
                    closest_id = current_id
                    closest_time = logged_time
                max_id = current_id - 1  # Move search downwards
                current_id = max([current_id - search_step, min_id])
            else:
                return closest_id_result, logged_time  # Exact match

            # Reduce Search step as we get close to the target
            search_step = max(1, search_step // 2)

        return closest_id, closest_time


def run():
    # --------------------------------------------------------------------------------- #
    # Inputs
    date_start = dt.datetime(year=2024, month=11, day=26)
    date_end = dt.datetime(year=2025, month=1, day=28)

    # ID Look Up Variables
    table_name = "ANAJQDW01.AnalyticsDW.Treatment"
    id_column = "TreatmentID"
    time_column = "LoggedUTC"
    look_up_object = IdLookUp()

    # Generate date range
    date_range = pl.date_range(start=date_start, end=date_end, interval="1d", eager=True).to_list()

    cursor = connection.cursor(as_dict=False)
    try:
        # Generate date hash
        batches = {}
        start_id = None
        for date in date_range:
            # Define start datetime and start endtime
            start_time = dt.datetime.combine(date, dt.datetime.min.time())
            end_time = dt.datetime.combine(date, dt.datetime.max.time())

            print("extracting for start time: ", start_time - dt.timedelta(hours=1))

            # Get Start id and corresponding startid datetime
            logged_start_id, logged_start_datetime = look_up_object.find_id_based_on_time(
                cursor,
                start_time - dt.timedelta(hours=1),
                table_name,
                id_column,
                time_column, "before", start_id, 100000000 if start_id is not None else None
            )
            print("extracting for end time: ", end_time + dt.timedelta(hours=1))
            start_id = logged_start_id
            # Get Start id and corresponding startid datetime
            logged_end_id, logged_end_datetime = look_up_object.find_id_based_on_time(
                cursor,
                end_time + dt.timedelta(hours=1),
                table_name,
                id_column,
                time_column, "after", start_id, 100000000 if start_id is not None else None
            )
            start_id = logged_end_id

            batches[date.strftime('%Y-%m-%d')] = {
                "date": date.strftime('%Y-%m-%d'),
                "start": start_time,  # Start of the day
                "end": end_time,  # End of the day
                "logged_start_id": logged_start_id,  # Start ID
                "logged_start_datetime": logged_start_datetime,  # Logged time for start ID
                "logged_end_id": logged_end_id,  # End ID
                "logged_end_datetime": logged_end_datetime  # Logged time for end ID
            }

        date_time_analysis = pl.DataFrame(list(batches.values())).sort("start")
        date_time_analysis.write_csv("target_date_time_ranges_japan.csv")
        cursor.close()
        connection.close()
    except Exception as err:
        print(traceback.format_exc())
        # Clean up connection to prevent rate limit
        cursor.close()
        connection.close()